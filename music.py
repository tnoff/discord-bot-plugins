from asyncio import sleep
from asyncio import Event, Queue, QueueEmpty, QueueFull, TimeoutError as asyncio_timeout
from copy import deepcopy
from datetime import datetime, timedelta
from functools import partial
from json import load as json_load
from json import dumps as json_dumps
from pathlib import Path
from random import shuffle as random_shuffle
from re import match as re_match
from shutil import copy as copy_file
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Optional
from uuid import uuid4

from async_timeout import timeout
from dappertable import shorten_string_cjk, DapperTable
from discord import FFmpegPCMAudio
from discord.errors import HTTPException, DiscordServerError, RateLimited
from discord.ext import commands
from moviepy.editor import AudioFileClip, afx
from numpy import sqrt
from requests import get as requests_get
from requests import post as requests_post
from sqlalchemy import desc
from sqlalchemy import Boolean, Column, DateTime, Integer, String
from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.exc import IntegrityError
from yt_dlp import YoutubeDL
from yt_dlp.postprocessor import PostProcessor
from yt_dlp.utils import DownloadError

from discord_bot.cogs.common import CogHelper
from discord_bot.database import BASE
from discord_bot.utils import async_retry_command

# Max title length for table views
MAX_STRING_LENGTH = 32

# Format for local cache file datetime
CACHE_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'

# Music defaults
DELETE_AFTER_DEFAULT = 300

# Max queue size
QUEUE_MAX_SIZE_DEFAULT = 128

# Max playlists per server (not including history)
SERVER_PLAYLIST_MAX_DEFAULT = 64

# Max song length
MAX_SONG_LENGTH_DEFAULT = 60 * 15

# Timeout for web requests
REQUESTS_TIMEOUT = 180

# Length of random play queue
DEFAULT_RANDOM_QUEUE_LENGTH = 32

# Timeout in seconds
DISCONNECT_TIMEOUT_DEFAULT = 60 * 15

# Max cache files to keep on disk
# NOTE: If you enable audio processing this keeps double the files as one gets edited
MAX_CACHE_FILES_DEFAULT = 2048

# Spotify
SPOTIFY_AUTH_URL = 'https://accounts.spotify.com/api/token'
SPOTIFY_BASE_URL = 'https://api.spotify.com/v1/'
YOUTUBE_BASE_URL =  'https://www.googleapis.com/youtube/v3/playlistItems'

SPOTIFY_PLAYLIST_REGEX = r'^https://open.spotify.com/playlist/(?P<playlist_id>([a-zA-Z0-9]+))(?P<extra_query>(\?[a-zA-Z0-9=&_-]+)?)(?P<shuffle>( *shuffle)?)'
SPOTIFY_ALBUM_REGEX = r'^https://open.spotify.com/album/(?P<album_id>([a-zA-Z0-9]+))(?P<extra_query>(\?[a-zA-Z0-9=&_-]+)?)(?P<shuffle>( *shuffle)?)'
YOUTUBE_PLAYLIST_REGEX = r'^https://(www.)?youtube.com/playlist\?list=(?P<playlist_id>[a-zA-Z0-9_-]+)(?P<shuffle> *(shuffle)?)'

NUMBER_REGEX = r'.*(?P<number>[0-9]+).*'

# We only care about the following data in the yt-dlp dict
YT_DLP_KEYS = ['id', 'title', 'webpage_url', 'uploader', 'duration']

#
# Common Functions
#

def max_song_filter_generator(max_song_length):
    '''
    Get function for filtering max song length
    '''
    def max_song_filter(info, *, incomplete): #pylint:disable=unused-argument
        '''
        Filter song based on max song length
        '''
        duration = info.get('duration')
        if duration and duration > max_song_length:
            raise SongTooLong(f'Song exceeds max length of {max_song_length}')
    return max_song_filter


def edit_audio_file(file_path):
    '''
    Normalize audio for file
    '''
    def get_finished_path(path):
        '''
        Get 'editing path' for editing files
        '''
        return path.parent / (path.stem + '.finished.mp3')

    def get_editing_path(path):
        '''
        Get 'editing path' for editing files
        '''
        return path.parent / (path.stem + '.edited.mp3')

    finished_path = get_finished_path(file_path)
    # If exists, assume it was already edited successfully
    if finished_path.exists():
        return finished_path
    editing_path = get_editing_path(file_path)
    audio_clip = AudioFileClip(str(file_path))
    # Find dead audio at start and end of file
    cut = lambda i: audio_clip.subclip(i, i+1).to_soundarray(fps=1)
    volume = lambda array: sqrt(((1.0 * array) ** 2).mean())
    volumes = [volume(cut(i)) for i in range(0, int(audio_clip.duration-1))]
    start = 0
    while True:
        if volumes[start] > 0:
            break
        start += 1
    end = len(volumes) - 1
    while True:
        if volumes[end] > 0:
            break
        end -= 1
    # From testing, it seems good to give this a little bit of a buffer, add 1 second to each end if possible
    if start > 0:
        start -= 1
    if end < audio_clip.duration - 1:
        end += 1
    audio_clip = audio_clip.subclip(t_start=start, t_end=end + 1)
    # Normalize audio
    edited_audio = audio_clip.fx(afx.audio_normalize) #pylint:disable=no-member
    edited_audio.write_audiofile(str(editing_path))
    editing_path.rename(finished_path)
    return finished_path


def rm_tree(pth):
    '''
    Remove all files in a tree
    '''
    # https://stackoverflow.com/questions/50186904/pathlib-recursively-remove-directory
    for child in pth.glob('*'):
        if child.is_file():
            child.unlink()
        else:
            rm_tree(child)
    pth.rmdir()

async def retry_discord_message_command(func, *args, **kwargs):
    '''
    Retry discord send message command, catch case of rate limiting
    '''
    def check_429(ex):
        if '429' not in str(ex):
            raise #pylint:disable=misplaced-bare-raise
    post_exception_functions = [check_429]
    exceptions = (HTTPException, RateLimited, DiscordServerError)
    return await async_retry_command(func, *args, **kwargs, accepted_exceptions=exceptions, post_exception_functions=post_exception_functions)

def json_converter(o): #pylint:disable=inconsistent-return-statements
    '''
    Convert json objects to proper strings for writing
    '''
    if isinstance(o, datetime):
        return o.strftime(CACHE_DATETIME_FORMAT)
    if isinstance(o, Path):
        return str(o)

#
# Music Tables
#

class Playlist(BASE):
    '''
    Playlist
    '''
    __tablename__ = 'playlist'
    __table_args__ = (
        UniqueConstraint('name', 'server_id',
                         name='_server_playlist'),
    )
    id = Column(Integer, primary_key=True)
    name = Column(String(256))
    server_id = Column(String(128))
    last_queued = Column(DateTime, nullable=True)
    created_at = Column(DateTime)
    is_history = Column(Boolean)


class PlaylistItem(BASE):
    '''
    Playlist Item
    '''
    __tablename__ = 'playlist_item'
    __table_args__ = (
        UniqueConstraint('video_id', 'playlist_id',
                         name='_unique_playlist_video'),
    )
    id = Column(Integer, primary_key=True)
    title = Column(String(256))
    video_id = Column(String(32))
    uploader = Column(String(256))
    playlist_id = Column(Integer, ForeignKey('playlist.id'))
    created_at = Column(DateTime)

#
# Exceptions
#

class PutsBlocked(Exception):
    '''
    Puts Blocked on Queue
    '''

class SongTooLong(Exception):
    '''
    Max length of song duration exceeded
    '''

class PlaylistMaxLength(Exception):
    '''
    Playlist hit max length
    '''
#
# Spotify Client
#

class SpotifyClient():
    '''
    Spotify Client for basic API Use
    '''
    def __init__(self, client_id, client_secret):
        '''
        Init spotify client
        '''
        self._token = None
        self._expiry = None
        self.client_id = client_id
        self.client_secret = client_secret

    @property
    def token(self):
        '''
        Fetch or generate token
        '''
        if self._token is None:
            self._refresh_token()
        elif self._expiry < datetime.now():
            self._refresh_token()
        return self._token

    def _refresh_token(self):
        '''
        Refresh token from spotify auth url
        '''
        auth_response = requests_post(SPOTIFY_AUTH_URL, {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
        }, timeout=REQUESTS_TIMEOUT)
        if auth_response.status_code != 200:
            raise Exception(f'Error getting auth token {auth_response.status_code}, {auth_response.text}')
        data = auth_response.json()
        self._token = data['access_token']
        self._expiry = datetime.now() + timedelta(seconds=data['expires_in'])

    def __gather_track_info(self, first_url):
        results = []
        url = first_url
        while True:
            r = requests_get(url, headers={'Authorization': f'Bearer {self.token}'}, timeout=REQUESTS_TIMEOUT)
            if r.status_code != 200:
                return r, results
            data = r.json()
            # May or may not have 'tracks' key
            try:
                data = data['tracks']
            except KeyError:
                pass
            for item in data['items']:
                # May or may not have 'track' key
                try:
                    item = item['track']
                except KeyError:
                    pass
                results.append({
                    'track_name': item['name'],
                    'track_artists': ', '.join(i['name'] for i in item['artists']),
                })
            try:
                url = data['tracks']['next']
            except KeyError:
                return r, results
        return r, results

    def playlist_get(self, playlist_id):
        '''
        Get playlist track info
        '''
        url = f'{SPOTIFY_BASE_URL}playlists/{playlist_id}'
        return self.__gather_track_info(url)

    def album_get(self, album_id):
        '''
        Get album track info
        '''
        url = f'{SPOTIFY_BASE_URL}albums/{album_id}'
        return self.__gather_track_info(url)

#
# Youtube API Client
#
class YoutubeAPI():
    '''
    Get info from youtube api
    '''
    def __init__(self, api_key):
        '''
        Init youtube api client
        api_key     :   Google developer api key
        '''
        self.api_key = api_key

    def playlist_list_items(self, playlist_id):
        '''
        List all video Ids in playlist
        playlist_id     :   ID of youtube playlist
        '''
        token = None
        results = []
        while True:
            url = f'{YOUTUBE_BASE_URL}?key={self.api_key}&playlistId={playlist_id}&part=snippet'
            if token:
                url = f'{url}&pageToken={token}'
            req = requests_get(url, timeout=REQUESTS_TIMEOUT)
            if req.status_code != 200:
                return req, results
            for item in req.json()['items']:
                if item['kind'] != 'youtube#playlistItem':
                    continue
                resource = item['snippet']['resourceId']
                if resource['kind'] != 'youtube#video':
                    continue
                results.append(resource['videoId'])
            try:
                token = req.json()['nextPageToken']
            except KeyError:
                return req, results
        return req, results

# Music bot setup
# Music taken from https://gist.github.com/EvieePy/ab667b74e9758433b3eb806c53a19f34
class MyQueue(Queue):
    '''
    Custom implementation of asyncio Queue
    '''
    def __init__(self, maxsize=0):
        self.shutdown = False
        super().__init__(maxsize=maxsize)

    def block(self):
        '''
        Block future puts, for when queue should be in shutdown
        '''
        self.shutdown = True

    def put_nowait(self, item):
        if self.shutdown:
            raise PutsBlocked('Puts Blocked on Queue')
        super().put_nowait(item)

    async def put(self, item):
        if self.shutdown:
            raise PutsBlocked('Puts Blocked on Queue')
        await super().put(item)

    def shuffle(self):
        '''
        Shuffle queue
        '''
        random_shuffle(self._queue)
        return True

    def clear(self):
        '''
        Remove all items from queue
        '''
        while self.qsize():
            self._queue.popleft()

    def remove_item(self, queue_index):
        '''
        Remove item from queue
        '''
        if queue_index < 1 or queue_index > self.qsize():
            return None
        # Rotate, remove top, then rotate back
        for _ in range(1, queue_index):
            self._queue.rotate(-1)
        item = self._queue.popleft()
        for _ in range(1, queue_index):
            self._queue.rotate(1)
        return item

    def bump_item(self, queue_index):
        '''
        Bump item to top of queue
        '''
        item = self.remove_item(queue_index)
        if item is not None:
            self._queue.appendleft(item)
        return item

#
# Source File
#

class SourceFile():
    '''
    Source file of downloaded content
    '''
    def __init__(self, file_path, ytdl_data, source_dict, logger):
        '''
        Init source file

        file_path                   :   Path to ytdl file
        ytdl_data                   :   Ytdl download dict
        source_dict                 :   Source dict passed to yt-dlp
        logger                      :   Python logger
        '''
        self.logger = logger
        # Keep only keys we want, has alot of metadata we dont care about
        self._new_dict = {}
        for key in YT_DLP_KEYS:
            try:
                self._new_dict[key] = ytdl_data[key]
            except KeyError:
                pass

        self._new_dict['requester'] = source_dict['requester']
        self._new_dict['guild_id'] = source_dict['guild_id']
        try:
            self._new_dict['added_from_history'] = source_dict['added_from_history']
        except KeyError:
            self._new_dict['added_from_history'] = False

        self.file_path = file_path
        self.original_path = file_path
        if self.file_path:
            # The modified time of download videos can be the time when it was actually uploaded to youtube
            # Touch here to update the modified time, so that the cleanup check works as intendend
            # Rename file to a random uuid name, that way we can have diff videos with same/similar names
            uuid_path = file_path.parent / f'{source_dict["guild_id"]}' / f'{uuid4()}{"".join(i for i in file_path.suffixes)}'
            copy_file(str(file_path), str(uuid_path))
            self.file_path = uuid_path
            self.logger.info(f'Music :: :: Moved downloaded url "{self._new_dict["webpage_url"]}" to file "{uuid_path}"')

    def __getitem__(self, key):
        '''
        Get attribute of dict
        '''
        if key == 'file_path':
            return self.file_path
        if key == 'original_path':
            return self.original_path
        return self._new_dict[key]

    def __setitem__(self, key, value):
        '''
        Set attributes of dict
        '''
        self._new_dict[key] = value

    def delete(self):
        '''
        Delete file
        '''
        if self.file_path.exists():
            self.file_path.unlink()

#
# YTDL Post Processor
#


class VideoEditing(PostProcessor):
    '''
    Run post processing on downloaded videos
    '''
    def run(self, information):
        '''
        Run post processing editing
        Get filename, edit with moviepy, and update dict
        '''
        file_path = Path(information['_filename'])
        file_path = edit_audio_file(file_path)
        information['_filename'] = str(file_path)
        information['filepath'] = str(file_path)
        return [], information

#
# Local Cache
#

class CacheFile():
    '''
    Keep cache of local files
    '''
    def __init__(self, cache_file, max_cache_files, logger):
        '''
        Create new file cache
        cache_file      :       Local cache json
        max_cache_files :       Maximum number of files to keep in cache
        logger          :       Python logger
        '''
        self._data = []
        self._file = cache_file
        self.max_cache_files = max_cache_files
        self.logger = logger
        if self._file.exists():
            with open(str(self._file), 'r') as o:
                self._data = json_load(o)

        # Check all files exist
        new_list = []
        for item in self._data:
            item['file_path'] = Path(item['file_path'])
            if not item['file_path'].exists():
                self.logger.warning(f'Music :: :: Cached file {str(item["file_path"])} does not exist, skipping')
                continue
            item['last_iterated_at'] = datetime.strptime(item['last_iterated_at'], CACHE_DATETIME_FORMAT)
            item['created_at'] = datetime.strptime(item['created_at'], CACHE_DATETIME_FORMAT)
            new_list.append(item)
        self._data = new_list
        self.logger.info(f'Music :: :: Cache starting with data {self._data}')

    def iterate_file(self, file_path):
        '''
        Bump file path
        file_path       :   Path of cached file
        '''
        self.logger.info(f'Music :: Adding file path {str(file_path)} to cache file')
        for item in self._data:
            if item['file_path'] == file_path:
                item['count'] += 1
                item['last_iterated_at'] = datetime.utcnow()
                self.logger.info(f'Music :: Cache entry existed for path {str(file_path)}, bumping')
                return
        now = datetime.utcnow()
        self.logger.info(f'Music :: Cache entry did not exist for path {str(file_path)}, creating now')
        self._data.append({
            'file_path': file_path,
            'count': 1,
            'last_iterated_at': now,
            'created_at': now,
        })

    def remove(self):
        '''
        Remove oldest and least used file from cache
        '''
        num_to_remove = len(self._data) - self.max_cache_files
        if num_to_remove < 1:
            self.logger.info(f'Music :: Total cache files {len(self._data)} and max is {self.max_cache_files}, no need to remove files')
            return
        self.logger.info(f'Music :: Need to remove {num_to_remove} cached files')
        sorted_list = sorted(self._data, key=lambda k: (float(k['count']), k['last_iterated_at']), reverse=False)
        removed = 0
        remove_files = []

        new_list = []
        for item in sorted_list:
            if removed < num_to_remove:
                remove_files.append(item)
                removed += 1
                continue
            new_list.append(item)

        for item in remove_files:
            self.logger.info(f'Music :: Removing item from cache {item}')
            if item['file_path'].exists():
                item['file_path'].unlink()
        self._data = new_list

    def write_file(self):
        '''
        Write to local file
        '''
        self._file.write_text(json_dumps(self._data, default=json_converter))


#
# YTDL Download Client
#

class DownloadClient():
    '''
    Download Client using yt-dlp
    '''
    def __init__(self, ytdl, logger, spotify_client=None, youtube_client=None,
                 delete_after=None):
        self.ytdl = ytdl
        self.logger = logger
        self.spotify_client = spotify_client
        self.youtube_client = youtube_client
        self.delete_after = delete_after

    def __prepare_data_source(self, source_dict, download=True):
        '''
        Prepare source from youtube url
        '''
        self.logger.info(f'Music :: Starting download of video "{source_dict["search_string"]}"')
        try:
            data = self.ytdl.extract_info(source_dict['search_string'], download=download)
        except DownloadError:
            self.logger.warning(f'Music :: Error downloading youtube search "{source_dict["search_string"]}')
            return None
        # Make sure we get the first entry here
        # Since we don't pass "url" directly anymore
        try:
            data = data['entries'][0]
        except IndexError:
            self.logger.warning(f'Music :: Error downloading youtube search "{source_dict["search_string"]}')
            return None
        except KeyError:
            pass

        file_path = None
        if download:
            try:
                file_path = Path(data['requested_downloads'][0]['filepath'])
                self.logger.info(f'Music :: Downloaded url "{data["webpage_url"]}" to file "{str(file_path)}"')
            except (KeyError, IndexError):
                self.logger.warning(f'Music :: Unable to get filepath from ytdl data {data}')
        return SourceFile(file_path, data, source_dict, self.logger)

    async def create_source(self, source_dict, loop, download=False):
        '''
        Download data from youtube search
        '''
        to_run = partial(self.__prepare_data_source, source_dict=source_dict, download=download)
        return await loop.run_in_executor(None, to_run)

    def __check_spotify_source(self, playlist_id=None, album_id=None):
        data = []
        if playlist_id:
            self.logger.debug(f'Music :: Checking for spotify playlist {playlist_id}')
            response, data = self.spotify_client.playlist_get(playlist_id)
            if response.status_code != 200:
                self.logger.warning(f'Music :: Unable to find spotify data {response.status_code}, {response.text}')
                return []
        if album_id:
            self.logger.debug(f'Music :: Checking for spotify album {album_id}')
            response, data = self.spotify_client.album_get(album_id)
            if response.status_code != 200:
                self.logger.warning(f'Music :: Unable to find spotify data {response.status_code}, {response.text}')
                return []
        search_strings = []
        for item in data:
            search_string = f'{item["track_name"]} {item["track_artists"]}'
            search_strings.append(search_string)
        return search_strings

    def __check_youtube_source(self, playlist_id=None):
        if playlist_id:
            self.logger.debug(f'Music :: Checking youtube playlist id {playlist_id}')
            response, data = self.youtube_client.playlist_list_items(playlist_id)
            if response.status_code != 200:
                self.logger.warning(f'Music :: Unable to find youtube playlist {response.status_code}, {response.text}')
                return []
            return data
        return []

    async def __check_source_types(self, search, loop):
        '''
        Check the source type of the search given

        If spotify type, grab info from spotify api and get proper search terms for youtube
        '''
        # If spotify, grab list of search strings, otherwise just grab single search
        spotify_playlist_matcher = re_match(SPOTIFY_PLAYLIST_REGEX, search)
        spotify_album_matcher = re_match(SPOTIFY_ALBUM_REGEX, search)
        playlist_matcher = re_match(YOUTUBE_PLAYLIST_REGEX, search)

        if spotify_playlist_matcher and self.spotify_client:
            to_run = partial(self.__check_spotify_source, playlist_id=spotify_playlist_matcher.group('playlist_id'))
            search_strings = await loop.run_in_executor(None, to_run)
            if spotify_playlist_matcher.group('shuffle'):
                random_shuffle(search_strings)
            self.logger.debug(f'Music :: Gathered {len(search_strings)} from spotify playlist "{search}"')
            return search_strings

        if spotify_album_matcher and self.spotify_client:
            to_run = partial(self.__check_spotify_source, album_id=spotify_album_matcher.group('album_id'))
            search_strings = await loop.run_in_executor(None, to_run)
            if spotify_album_matcher.group('shuffle'):
                random_shuffle(search_strings)
            self.logger.debug(f'Music :: Gathered {len(search_strings)} from spotify playlist "{search}"')
            return search_strings

        if playlist_matcher and self.youtube_client:
            to_run = partial(self.__check_youtube_source, playlist_id=playlist_matcher.group('playlist_id'))
            search_strings = await loop.run_in_executor(None, to_run)
            if playlist_matcher.group('shuffle'):
                random_shuffle(search_strings)
            self.logger.debug(f'Music :: Gathered {len(search_strings)} from youtube playlist "{search}"')
            return search_strings
        return [search]

    async def check_source(self, search, guild_id, requester_name, loop):
        '''
        Create source from youtube search
        '''
        search_strings = await self.__check_source_types(search, loop)

        all_entries = []
        for search_string in search_strings:
            all_entries.append({
                'guild_id': guild_id,
                'requester': requester_name,
                'search_string': search_string,
            })
        return all_entries

#
# Music Player for channel
#

class MusicPlayer:
    '''
    A class which is assigned to each guild using the bot for Music.

    This class implements a queue and loop, which allows for different guilds
    to listen to different playlists simultaneously.

    When the bot disconnects from the Voice it's instance will be destroyed.
    '''

    def __init__(self, bot, guild, cog_cleanup, text_channel, voice_channel, logger,
                 download_client, cache_file, max_song_length, queue_max_size, delete_after, history_playlist_id, disconnect_timeout):
        self.bot = bot
        self.logger = logger
        self.guild = guild
        self.text_channel = text_channel
        self.voice_channel = voice_channel
        self.cog_cleanup = cog_cleanup
        self.download_client = download_client
        self.cache_file = cache_file
        self.delete_after = delete_after
        self.history_playlist_id = history_playlist_id
        self.disconnect_timeout = disconnect_timeout

        self.download_queue = MyQueue(maxsize=queue_max_size)
        self.play_queue = MyQueue(maxsize=queue_max_size)
        self.history = MyQueue(maxsize=queue_max_size)
        self.next = Event()

        self.np_message = ''
        self.song_skipped = False
        self.queue_messages = [] # Show current queue
        self.volume = 0.5
        self.max_song_length = max_song_length

        # For showing messages
        self.lock_file = Path(NamedTemporaryFile(delete=False).name) #pylint:disable=consider-using-with

        self._player_task = None
        self._download_task = None

    async def start(self):
        '''
        Start background methods
        '''
        self._player_task = self.bot.loop.create_task(self.player_loop())
        self._download_task = self.bot.loop.create_task(self.download_files())

    async def acquire_lock(self, wait_timeout=600):
        '''
        Wait for and acquire lock
        '''
        start = datetime.now()
        while True:
            if (datetime.now() - start).seconds > wait_timeout:
                raise Exception('Error acquiring player lock lock')
            if self.lock_file.read_text() == 'locked':
                await sleep(.5)
                continue
            break
        self.lock_file.write_text('locked')

    async def release_lock(self):
        '''
        Release lock
        '''
        self.lock_file.write_text('unlocked')

    async def should_delete_messages(self):
        '''
        Check if known queue messages match whats in channel history
        '''
        num_messages = len(self.queue_messages)
        history = [message async for message in self.text_channel.history(limit=num_messages)]
        for (count, hist_item) in enumerate(history):
            mess = self.queue_messages[num_messages - 1 - count]
            if mess.id != hist_item.id:
                return True
        return False

    async def clear_queue_messages(self):
        '''
        Delete queue messages
        '''
        await self.acquire_lock()
        for queue_message in self.queue_messages:
            await retry_discord_message_command(queue_message.delete)
        self.queue_messages = []
        await self.release_lock()

    def get_queue_message(self):
        '''
        Get full queue message
        '''
        items = []
        if self.np_message:
            items.append(self.np_message)
        if not self.play_queue._queue: #pylint:disable=protected-access
            return items
        headers = [
            {
                'name': 'Pos',
                'length': 3,
            },
            {
                'name': 'Wait Time',
                'length': 9,
            },
            {
                'name': 'Title',
                'length': 64,
            },
            {
                'name': 'Uploader',
                'length': 32,
            },
        ]
        table = DapperTable(headers, rows_per_message=15)
        duration = 0
        queue_items = deepcopy(self.play_queue._queue) #pylint:disable=protected-access
        for (count, item) in enumerate(queue_items):
            uploader = item['uploader'] or ''
            delta = timedelta(seconds=duration)
            duration += item['duration']
            table.add_row([
                f'{count + 1}',
                f'{str(delta)}',
                item['title'],
                uploader,
            ])
        for t in table.print():
            items.append(f'```{t}```')
        return items

    async def update_queue_strings(self):
        '''
        Update queue message in channel
        '''
        await self.acquire_lock()
        if self.play_queue.shutdown:
            await self.release_lock()
            return

        delete_messages = await self.should_delete_messages()
        self.logger.debug(f'Music :: Updating queue messages in channel {self.text_channel.id} in guild {self.guild.id}')
        new_queue_strings = self.get_queue_message() or []
        if delete_messages:
            for queue_message in self.queue_messages:
                await retry_discord_message_command(queue_message.delete)
            self.queue_messages = []
        elif len(self.queue_messages) > len(new_queue_strings):
            for _ in range(len(self.queue_messages) - len(new_queue_strings)):
                queue_message = self.queue_messages.pop(-1)
                await retry_discord_message_command(queue_message.delete)
        for (count, queue_message) in enumerate(self.queue_messages):
            # Check if queue message is the same before updating
            if queue_message.content == new_queue_strings[count]:
                continue
            await retry_discord_message_command(queue_message.edit, content=new_queue_strings[count])
        if len(self.queue_messages) < len(new_queue_strings):
            for table in new_queue_strings[-(len(new_queue_strings) - len(self.queue_messages)):]:
                self.queue_messages.append(await retry_discord_message_command(self.text_channel.send, table))
        await self.release_lock()

    async def download_files(self):
        '''
        Go through download loop and download all files
        '''
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():
            # Await a sleep here just so other tasks can grab loop
            await sleep(.01)
            if self.play_queue.shutdown:
                self.logger.warning(f'Music ::: Play queue in shutdown, skipping downloads for guild {self.guild.id}')
                return
            self.logger.debug(f'Music ::: Waiting on new download item for guild {self.guild.id}')
            source_dict = await self.download_queue.get()
            self.logger.debug(f'Music ::: Gathered new item to download "{source_dict["search_string"]}"')

            if self.play_queue.full():
                self.logger.warning(f'Music ::: Play queue full, aborting download of item "{source_dict["search_string"]}"')
                await retry_discord_message_command(source_dict['message'].edit,
                                                    content=f'Play queue is full, cannot add "{source_dict["search_string"]}"',
                                                    delete_after=self.delete_after)
                continue

            try:
                source_download = await self.download_client.create_source(source_dict, self.bot.loop, download=True)
            except SongTooLong:
                await retry_discord_message_command(source_dict['message'].edit,
                                                    content=f'Search "{source_dict["search_string"]}" exceeds maximum of {self.max_song_length} seconds, skipping',
                                                    delete_after=self.delete_after)
                self.logger.warning(f'Music ::: Song too long to play in queue, skipping "{source_dict["search_string"]}"')
                continue
            if source_download is None:
                await retry_discord_message_command(source_dict['message'].edit, content=f'Issue downloading video "{source_dict["search_string"]}", skipping',
                                                    delete_after=self.delete_after)
                continue
            try:
                self.play_queue.put_nowait(source_download)
                self.logger.info(f'Music :: Adding "{source_download["title"]}" '
                                 f'to queue in guild {source_dict["guild_id"]}')
                await self.update_queue_strings()
                await retry_discord_message_command(source_dict['message'].delete)
            except QueueFull:
                self.logger.warning(f'Music ::: Play queue full, aborting download of item "{source_dict["search_string"]}"')
                await retry_discord_message_command(source_dict['message'].edit,
                                                    content=f'Play queue is full, cannot add "{source_dict["search_string"]}"',
                                                    delete_after=self.delete_after)
                source_download.delete()
                # Dont return to loop, file was downloaded so we can iterate on cache at least
            except PutsBlocked:
                self.logger.warning(f'Music :: Puts Blocked on queue in guild "{source_dict["guild_id"]}", assuming shutdown')
                await retry_discord_message_command(source_dict['message'].delete)
                source_download.delete()
                continue
            # Iterate on cache file if exists
            if self.cache_file:
                self.logger.info(f'Music :: Iterating file on original path {str(source_download["original_path"])}')
                self.cache_file.iterate_file(source_download['original_path'])
                self.logger.debug('Music ::: Checking cache files to remove in music player')
                self.cache_file.remove()
                self.cache_file.write_file()

    def set_next(self, *_args, **_kwargs):
        '''
        Used for loop to call once voice channel done
        '''
        self.next.set()

    async def player_loop(self):
        '''
        Our main player loop.
        '''
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():
            self.next.clear()

            try:
                # Wait for the next song. If we timeout cancel the player and disconnect...
                async with timeout(self.disconnect_timeout):
                    source = await self.play_queue.get()
            except asyncio_timeout:
                self.logger.info(f'Music :: bot reached timeout on queue in guild "{self.guild.name}"')
                return await self.destroy(self.guild)

            # Double check file didnt go away
            if not source['file_path'].exists():
                await retry_discord_message_command(self.text_channel.send, f'Unable to play "{source["title"]}", local file dissapeared')
                continue

            audio_source = FFmpegPCMAudio(str(source['file_path']))
            self.song_skipped = False
            audio_source.volume = self.volume
            try:
                self.guild.voice_client.play(audio_source, after=self.set_next) #pylint:disable=line-too-long
            except AttributeError:
                self.logger.info(f'Music :: No voice client found, disconnecting from guild {self.guild.name}')
                return await self.destroy(self.guild)
            self.logger.info(f'Music :: Now playing "{source["title"]}" requested '
                             f'by "{source["requester"]}" in guild {self.guild.id}, url '
                             f'"{source["webpage_url"]}"')
            self.np_message = f'Now playing {source["webpage_url"]} requested by {source["requester"]}'
            await self.update_queue_strings()

            await self.next.wait()

            # Make sure the FFmpeg process is cleaned up.
            audio_source.cleanup()
            source.delete()

            # Add song to history if possible
            if not self.song_skipped:
                try:
                    self.history.put_nowait(source)
                except QueueFull:
                    await self.history.get()
                    self.history.put_nowait(source)

            # If play queue empty, set np message to nill
            if self.play_queue.empty() and self.download_queue.empty():
                self.np_message = ''
                await self.update_queue_strings()

    async def clear_remaining_queue(self):
        '''
        Delete files downloaded for queue
        '''
        # Block puts first on download queue
        self.download_queue.block()
        self.play_queue.block()
        # Wait to ensure we have the block set
        await sleep(.5)
        messages = []
        # Delete any messages from download queue
        # Delete any files in play queue that are already added
        while True:
            try:
                source = self.download_queue.get_nowait()
                messages.append(source['message'])
            except QueueEmpty:
                break
        while True:
            try:
                source = self.play_queue.get_nowait()
                source.delete()
            except QueueEmpty:
                break
        # Grab history items
        history_items = []
        while True:
            try:
                item = self.history.get_nowait()
                # If item wasn't history originally, track it for the history playlist
                if not item['added_from_history']:
                    history_items.append(item)
            except QueueEmpty:
                break
        # Clear out all the queues
        self.history.clear()
        self.download_queue.clear()
        self.play_queue.clear()
        # Delete any outstanding download message
        for message in messages:
            await retry_discord_message_command(message.delete)
        # Cancel any tasks, delete lock file
        if self._player_task:
            self._player_task.cancel()
        if self._download_task:
            self._download_task.cancel()
        if self.lock_file.exists():
            self.lock_file.unlink()
        return history_items

    async def destroy(self, guild):
        '''
        Disconnect and cleanup the player.
        '''
        self.logger.info(f'Music :: Removing music bot from guild "{self.guild.name}", id {self.guild.id}')
        await self.cog_cleanup(guild)


class Music(CogHelper): #pylint:disable=too-many-public-methods
    '''
    Music related commands
    '''

    def __init__(self, bot, db_engine, logger, settings):
        super().__init__(bot, db_engine, logger, settings)
        BASE.metadata.create_all(self.db_engine)
        BASE.metadata.bind = self.db_engine
        self.logger = logger
        self.players = {}
        self.delete_after = settings.get('music_message_delete_after', DELETE_AFTER_DEFAULT)
        self.queue_max_size = settings.get('music_queue_max_size', QUEUE_MAX_SIZE_DEFAULT)
        self.server_playlist_max = settings.get('music_server_playlist_max', SERVER_PLAYLIST_MAX_DEFAULT)
        self.max_song_length = settings.get('music_max_song_length', MAX_SONG_LENGTH_DEFAULT)
        self.disconnect_timeout = settings.get('music_disconnect_timeout', DISCONNECT_TIMEOUT_DEFAULT)
        self.download_dir = settings.get('music_download_dir', None)
        self.enable_audio_processing = settings.get('music_enable_audio_processing', False)
        self.enable_cache = settings.get('music_enable_cache_files', False)
        self.max_cache_files = settings.get('music_max_cache_files', MAX_CACHE_FILES_DEFAULT)
        spotify_client_id = settings.get('music_spotify_client_id', None)
        spotify_client_secret = settings.get('music_spotify_client_secret', None)
        youtube_api_key = settings.get('music_youtube_api_key', None)
        self.spotify_client = None
        if spotify_client_id and spotify_client_secret:
            self.spotify_client = SpotifyClient(spotify_client_id, spotify_client_secret)

        self.youtube_client = None
        if youtube_api_key:
            self.youtube_client = YoutubeAPI(youtube_api_key)

        if self.download_dir is not None:
            self.download_dir = Path(self.download_dir)
            if not self.download_dir.exists():
                self.download_dir.mkdir(exist_ok=True, parents=True)
        else:
            self.download_dir = Path(TemporaryDirectory().name) #pylint:disable=consider-using-with

        self.cache_file = None
        if self.enable_cache:
            self.cache_file = CacheFile(self.download_dir / 'cache.json', self.max_cache_files, self.logger)

        ytdlopts = {
            'format': 'bestaudio',
            'restrictfilenames': True,
            'noplaylist': True,
            'nocheckcertificate': True,
            'ignoreerrors': False,
            'logtostderr': False,
            'logger': self.logger,
            'default_search': 'auto',
            'source_address': '0.0.0.0',  # ipv6 addresses cause issues sometimes
            'outtmpl': str(self.download_dir / '%(id)s.%(ext)s'),
        }
        if self.max_song_length:
            ytdlopts['match_filter'] = max_song_filter_generator(self.max_song_length)
        ytdl = YoutubeDL(ytdlopts)
        if self.enable_audio_processing:
            ytdl.add_post_processor(VideoEditing(), when='post_process')
        self.download_client = DownloadClient(ytdl, self.logger,
                                              spotify_client=self.spotify_client, youtube_client=self.youtube_client,
                                              delete_after=self.delete_after)

    async def cog_unload(self):
        '''
        Run when cog stops
        '''
        guild_list = list(self.players.keys())
        for guild in guild_list:
            await self.cleanup(guild)
        # Only remove download dir if cache not enabled
        if self.download_dir.exists() and not self.enable_cache:
            rm_tree(self.download_dir)

    async def __check_database_session(self, ctx):
        '''
        Check if database session is in use
        '''
        if not self.db_session:
            await retry_discord_message_command(ctx.send, 'Functionality not available, database is not enabled')
            return False
        return True

    async def cleanup(self, guild):
        '''
        Cleanup guild player
        '''
        try:
            await guild.voice_client.disconnect()
        except AttributeError:
            pass

        try:
            player = self.players[guild.id]
        except KeyError:
            return

        await player.clear_queue_messages()

        history_items = await player.clear_remaining_queue()
        if player.history_playlist_id:
            playlist = self.db_session.query(Playlist).get(player.history_playlist_id)
            for item in history_items:
                while True:
                    try:
                        self.__playlist_add_item(playlist, item['id'], item['title'], item['uploader'], ignore_fail=True)
                        break
                    except PlaylistMaxLength:
                        deleted_item =  self.db_session.query(PlaylistItem).\
                                            filter(PlaylistItem.playlist_id == playlist.id).\
                                            order_by(desc(PlaylistItem.created_at)).first()
                        if deleted_item:
                            self.db_session.delete(deleted_item)
                            self.db_session.commit()

        guild_path = self.download_dir / f'{guild.id}'
        if guild_path.exists():
            rm_tree(guild_path)

        # See if we need to delete
        try:
            del self.players[guild.id]
        except KeyError:
            pass

    async def get_player(self, ctx, voice_channel):
        '''
        Retrieve the guild player, or generate one.
        '''
        try:
            player = self.players[ctx.guild.id]
        except KeyError:
            # Make directory for guild specific files
            guild_path = self.download_dir / f'{ctx.guild.id}'
            guild_path.mkdir(exist_ok=True, parents=True)
            # Create history playlist if db session set
            history_playlist_id = None
            if self.db_session:
                history_playlist = self.db_session.query(Playlist).\
                    filter(Playlist.server_id == str(ctx.guild.id)).\
                    filter(Playlist.is_history == True).first()

                if not history_playlist:
                    history_playlist = Playlist(name=f'__playhistory__{ctx.guild.id}',
                                                server_id=ctx.guild.id,
                                                created_at=datetime.utcnow(),
                                                is_history=True)
                    self.db_session.add(history_playlist)
                    self.db_session.commit()
                history_playlist_id = history_playlist.id
            # Generate and start player
            player = MusicPlayer(ctx.bot, ctx.guild, ctx.cog.cleanup, ctx.channel, voice_channel,
                                 self.logger, self.download_client, self.cache_file, self.max_song_length,
                                 self.queue_max_size, self.delete_after, history_playlist_id, self.disconnect_timeout)
            await player.start()
            self.players[ctx.guild.id] = player

        return player

    async def __check_author_voice_chat(self, ctx, check_voice_chats=True):
        '''
        Check that command author in proper voice chat
        '''
        try:
            channel = ctx.author.voice.channel
        except AttributeError:
            await retry_discord_message_command(ctx.send, f'"{ctx.author.name}" not in voice chat channel. Please join one and try again',
                                                delete_after=self.delete_after)
            return None

        if not check_voice_chats:
            return channel

        if channel.guild.id is not ctx.guild.id:
            await retry_discord_message_command(ctx.send, 'User not joined to channel bot is in, ignoring command',
                                     delete_after=self.delete_after)
            return False
        return channel

    @commands.command(name='join', aliases=['awaken'])
    async def connect_(self, ctx):
        '''
        Connect to voice channel.
        '''
        if not await self.check_user_role(ctx):
            return await retry_discord_message_command(ctx.send, 'Unable to verify user role, ignoring command', delete_after=self.delete_after)

        channel = await self.__check_author_voice_chat(ctx, check_voice_chats=False)
        vc = ctx.voice_client

        if vc:
            if vc.channel.id == channel.id:
                return
            try:
                self.logger.info(f'Music :: bot moving to channel {channel.id} '
                                 f'in guild {ctx.guild.id}')
                await vc.move_to(channel)
            except asyncio_timeout:
                self.logger.warning(f'Music :: Moving to channel {channel.id} timed out')
                return await retry_discord_message_command(ctx.send, f'Moving to channel: <{channel}> timed out.')
        else:
            try:
                await channel.connect()
            except asyncio_timeout:
                self.logger.error(f'Music :: Connecting to channel {channel.id} timed out')
                return await retry_discord_message_command(ctx.send, f'Connecting to channel: <{channel}> timed out.')

        await retry_discord_message_command(ctx.send, f'Connected to: {channel}', delete_after=self.delete_after)

    @commands.command(name='play')
    async def play_(self, ctx, *, search: str):
        '''
        Request a song and add it to the queue.

        search: str [Required]
            The song to search and retrieve from youtube.
            This could be a simple search, an ID or URL.
        '''
        if not await self.check_user_role(ctx):
            return await retry_discord_message_command(ctx.send, 'Unable to verify user role, ignoring command', delete_after=self.delete_after)
        if not await self.__check_author_voice_chat(ctx):
            return
        vc = ctx.voice_client

        if not vc:
            await ctx.invoke(self.connect_)
            vc = ctx.voice_client

        player = await self.get_player(ctx, vc.channel)

        if player.play_queue.full():
            return await retry_discord_message_command(ctx.send, 'Queue is full, cannot add more songs',
                                                       delete_after=self.delete_after)

        entries = await player.download_client.check_source(search, ctx.guild.id, ctx.author.name, self.bot.loop)
        for entry in entries:
            try:
                message = await retry_discord_message_command(ctx.send, f'Downloading and processing "{entry["search_string"]}"')
                self.logger.debug(f'Music :: Handing off entry {entry} to download queue')
                entry['message'] = message
                player.download_queue.put_nowait(entry)
            except PutsBlocked:
                self.logger.warning(f'Music :: Puts to queue in guild {ctx.guild.id} are currently blocked, assuming shutdown')
                await retry_discord_message_command(message.delete)
                return
            except QueueFull:
                await retry_discord_message_command(ctx.send, f'Unable to add "{search}" to queue, download queue is full', delete_after=self.delete_after)
                await retry_discord_message_command(message.delete)
                return

    @commands.command(name='skip')
    async def skip_(self, ctx):
        '''
        Skip the song.
        '''
        if not await self.check_user_role(ctx):
            return await retry_discord_message_command(ctx.send, 'Unable to verify user role, ignoring command', delete_after=self.delete_after)
        if not await self.__check_author_voice_chat(ctx):
            return
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await retry_discord_message_command(ctx.send, 'I am not currently playing anything',
                                            delete_after=self.delete_after)

        player = await self.get_player(ctx, vc.channel)
        if not vc.is_paused() and not vc.is_playing():
            return
        player.song_skipped = True
        vc.stop()
        await retry_discord_message_command(ctx.send, 'Skipping song',
                                            delete_after=self.delete_after)

    @commands.command(name='clear')
    async def clear(self, ctx):
        '''
        Clear all items from queue
        '''
        if not await self.check_user_role(ctx):
            return await retry_discord_message_command(ctx.send, 'Unable to verify user role, ignoring command', delete_after=self.delete_after)
        if not await self.__check_author_voice_chat(ctx):
            return
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await retry_discord_message_command(ctx.send, 'I am not currently playing anything',
                                                       delete_after=self.delete_after)

        player = await self.get_player(ctx, vc.channel)
        if player.play_queue.empty():
            return await retry_discord_message_command(ctx.send, 'There are currently no more queued songs.',
                                                      delete_after=self.delete_after)
        queue_copy = deepcopy(player.play_queue)
        player.play_queue.clear()
        await retry_discord_message_command(ctx.send, 'Cleared all items from queue',
                                            delete_after=self.delete_after)

        # Reset queue messages
        await player.clear_queue_messages()
        # Make sure we delete all the old files
        while True:
            try:
                source = queue_copy.get_nowait()
                source.delete()
            except QueueEmpty:
                break

    @commands.command(name='history')
    async def history_(self, ctx):
        '''
        Show recently played songs
        '''
        if not await self.check_user_role(ctx):
            return await retry_discord_message_command(ctx.send, 'Unable to verify user role, ignoring command', delete_after=self.delete_after)
        if not await self.__check_author_voice_chat(ctx):
            return
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await retry_discord_message_command(ctx.send, 'I am not currently playing anything',
                                            delete_after=self.delete_after)

        player = await self.get_player(ctx, vc.channel)
        if player.history.empty():
            return await retry_discord_message_command(ctx.send, 'There have been no songs played.',
                                                       delete_after=self.delete_after)

        headers = [
            {
                'name': 'Pos',
                'length': 3,
            },
            {
                'name': 'Title',
                'length': 64,
            },
            {
                'name': 'Uploader',
                'length': 32,
            },
        ]
        table = DapperTable(headers, rows_per_message=15)
        table_items = deepcopy(player.history._queue) #pylint:disable=protected-access
        for (count, item) in enumerate(table_items):
            uploader = item['uploader'] or ''
            table.add_row([
                f'{count + 1}',
                item['title'],
                uploader,
            ])
        messages = [f'```{t}```' for t in table.print()]
        for mess in messages:
            await retry_discord_message_command(ctx.send, mess, delete_after=self.delete_after)

    @commands.command(name='shuffle')
    async def shuffle_(self, ctx):
        '''
        Shuffle song queue.
        '''
        if not await self.check_user_role(ctx):
            return await retry_discord_message_command(ctx.send, 'Unable to verify user role, ignoring command', delete_after=self.delete_after)
        if not await self.__check_author_voice_chat(ctx):
            return
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await retry_discord_message_command(ctx.send, 'I am not currently playing anything',
                                                       delete_after=self.delete_after)

        player = await self.get_player(ctx, vc.channel)
        if player.play_queue.empty():
            return await retry_discord_message_command(ctx.send, 'There are currently no more queued songs.',
                                                       delete_after=self.delete_after)
        player.play_queue.shuffle()

        await player.update_queue_strings()

    @commands.command(name='remove')
    async def remove_item(self, ctx, queue_index):
        '''
        Remove item from queue.

        queue_index: integer [Required]
            Position in queue of song that will be removed.
        '''
        if not await self.check_user_role(ctx):
            return await retry_discord_message_command(ctx.send, 'Unable to verify user role, ignoring command', delete_after=self.delete_after)
        if not await self.__check_author_voice_chat(ctx):
            return
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await retry_discord_message_command(ctx.send, 'I am not currently connected to voice',
                                            delete_after=self.delete_after)

        player = await self.get_player(ctx, vc.channel)
        if player.play_queue.empty():
            return await retry_discord_message_command(ctx.send, 'There are currently no more queued songs.',
                                            delete_after=self.delete_after)

        try:
            queue_index = int(queue_index)
        except ValueError:
            return await retry_discord_message_command(ctx.send, f'Invalid queue index {queue_index}',
                                            delete_after=self.delete_after)

        item = player.play_queue.remove_item(queue_index)
        if item is None:
            return retry_discord_message_command(ctx.send, f'Unable to remove queue index {queue_index}',
                            delete_after=self.delete_after)
        await retry_discord_message_command(ctx.send, f'Removed item {item["title"]} from queue',
                                 delete_after=self.delete_after)
        item.delete()
        await player.update_queue_strings()

    @commands.command(name='bump')
    async def bump_item(self, ctx, queue_index):
        '''
        Bump item to top of queue

        queue_index: integer [Required]
            Position in queue of song that will be removed.
        '''
        if not await self.check_user_role(ctx):
            return await retry_discord_message_command(ctx.send, 'Unable to verify user role, ignoring command', delete_after=self.delete_after)
        if not await self.__check_author_voice_chat(ctx):
            return
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await retry_discord_message_command(ctx.send, 'I am not currently connected to voice',
                                            delete_after=self.delete_after)

        player = await self.get_player(ctx, vc.channel)
        if player.play_queue.empty():
            return await retry_discord_message_command(ctx.send, 'There are currently no more queued songs.',
                                            delete_after=self.delete_after)

        try:
            queue_index = int(queue_index)
        except ValueError:
            return await retry_discord_message_command(ctx.send, f'Invalid queue index {queue_index}',
                                            delete_after=self.delete_after)

        item = player.play_queue.bump_item(queue_index)
        if item is None:
            return await retry_discord_message_command(ctx.send, f'Unable to bump queue index {queue_index}',
                                            delete_after=self.delete_after)
        await retry_discord_message_command(ctx.send, f'Bumped item {item["title"]} to top of queue',
                                 delete_after=self.delete_after)

        await player.update_queue_strings()

    @commands.command(name='stop')
    async def stop_(self, ctx):
        '''
        Stop the currently playing song and disconnect bot from voice chat.
        '''
        if not await self.check_user_role(ctx):
            return await retry_discord_message_command(ctx.send, 'Unable to verify user role, ignoring command', delete_after=self.delete_after)
        if not await self.__check_author_voice_chat(ctx):
            return
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await retry_discord_message_command(ctx.send, 'I am not currently playing anything',
                                            delete_after=self.delete_after)

        await self.cleanup(ctx.guild)

    async def __get_playlist(self, playlist_index, ctx):
        try:
            index = int(playlist_index)
        except ValueError:
            await retry_discord_message_command(ctx.send, f'Invalid playlist index {playlist_index}', delete_after=self.delete_after)
            return None
        playlist_items = self.db_session.query(Playlist).\
            filter(Playlist.server_id == str(ctx.guild.id)).order_by(Playlist.created_at.asc())
        playlist_items = [p for p in playlist_items if '__playhistory__' not in p.name]

        if not playlist_items:
            await retry_discord_message_command(ctx.send, 'No playlists in database',
                                                delete_after=self.delete_after)
            return None
        try:
            return playlist_items[index - 1]
        except IndexError:
            await retry_discord_message_command(ctx.send, f'Invalid playlist index {playlist_index}', delete_after=self.delete_after)
            return None

    @commands.group(name='playlist', invoke_without_command=False)
    async def playlist(self, ctx):
        '''
        Playlist functions.
        '''
        if ctx.invoked_subcommand is None:
            await retry_discord_message_command(ctx.send, 'Invalid sub command passed...', delete_after=self.delete_after)

    async def __playlist_create(self, ctx, name):
        if not await self.check_user_role(ctx):
            return await retry_discord_message_command(ctx.send, 'Unable to verify user role, ignoring command', delete_after=self.delete_after)
        if not await self.__check_author_voice_chat(ctx):
            return
        if not await self.__check_database_session(ctx):
            retry_discord_message_command(ctx.send, 'Database not set, cannot use playlist functions', delete_after=self.delete_after)
            return None
        # Check name doesn't conflict with history
        playlist_name = shorten_string_cjk(name, 256)
        if '__playhistory__' in playlist_name.lower():
            await retry_discord_message_command(ctx.send, f'Unable to create playlist "{name}", name cannot contain __playhistory__')
            return None
        # Check we haven't hit max playlist for server
        server_playlist_count = self.db_session.query(Playlist).filter(Playlist.server_id == str(ctx.guild.id)).count()
        if server_playlist_count >= self.server_playlist_max:
            await retry_discord_message_command(ctx.send, f'Unable to create playlist "{name}", already hit max playlists for server')
            return None
        playlist = Playlist(name=playlist_name,
                            server_id=ctx.guild.id,
                            created_at=datetime.utcnow(),
                            is_history=False)
        try:
            self.db_session.add(playlist)
            self.db_session.commit()
        except IntegrityError:
            self.db_session.rollback()
            self.db_session.commit()
            await retry_discord_message_command(ctx.send, f'Unable to create playlist "{name}", name likely already exists')
            return None
        self.logger.info(f'Music :: Playlist created "{playlist.name}" with ID {playlist.id} in guild {ctx.guild.id}')
        await retry_discord_message_command(ctx.send, f'Created playlist "{name}"',
                                            delete_after=self.delete_after)
        return playlist

    @playlist.command(name='create')
    async def playlist_create(self, ctx, *, name: str):
        '''
        Create new playlist.

        name: str [Required]
            Name of new playlist to create
        '''
        await self.__playlist_create(ctx, name)

    @playlist.command(name='list')
    async def playlist_list(self, ctx):
        '''
        List playlists.
        '''

        if not await self.check_user_role(ctx):
            return await retry_discord_message_command(ctx.send, 'Unable to verify user role, ignoring command', delete_after=self.delete_after)
        if not await self.__check_author_voice_chat(ctx):
            return
        if not await self.__check_database_session(ctx):
            return retry_discord_message_command(ctx.send, 'Database not set, cannot use playlist functions', delete_after=self.delete_after)
        playlist_items = self.db_session.query(Playlist).\
            filter(Playlist.server_id == str(ctx.guild.id)).order_by(Playlist.created_at.asc())
        playlist_items = [p for p in playlist_items if '__playhistory__' not in p.name]

        if not playlist_items:
            return await retry_discord_message_command(ctx.send, 'No playlists in database',
                                            delete_after=self.delete_after)

        headers = [
            {
                'name': 'ID',
                'length': 3,
            },
            {
                'name': 'Playlist Name',
                'length': 64,
            },
            {
                'name': 'Last Queued',
                'length': 20,
            }
        ]
        table = DapperTable(headers, rows_per_message=15)
        for (count, item) in enumerate(playlist_items):
            last_queued = 'N/A'
            if item.last_queued:
                last_queued = item.last_queued.strftime('%Y-%m-%d %H:%M:%S')
            table.add_row([
                f'{count + 1}',
                item.name,
                last_queued,
            ])
        messages = [f'```{t}```' for t in table.print()]
        for mess in messages:
            await retry_discord_message_command(ctx.send, mess, delete_after=self.delete_after)

    def __playlist_add_item(self, playlist, data_id, data_title, data_uploader, ignore_fail=False):
        self.logger.info(f'Music :: Adding video_id {data_id} to playlist {playlist.id}')
        item_count = self.db_session.query(PlaylistItem).filter(PlaylistItem.playlist_id == playlist.id).count()
        if item_count >= (self.server_playlist_max):
            raise PlaylistMaxLength(f'Playlist {playlist.id} greater to or equal to max length {self.server_playlist_max}')

        playlist_item = PlaylistItem(title=shorten_string_cjk(data_title, 256),
                                     video_id=data_id,
                                     uploader=shorten_string_cjk(data_uploader, 256),
                                     playlist_id=playlist.id,
                                     created_at=datetime.utcnow())
        try:
            self.db_session.add(playlist_item)
            self.db_session.commit()
            return playlist_item
        except IntegrityError as e:
            if not ignore_fail:
                self.logger.exception(e)
                self.logger.warning(str(e))
            self.db_session.rollback()
            self.db_session.commit()
            return None

    @playlist.command(name='item-add')
    async def playlist_item_add(self, ctx, playlist_index, *, search: str):
        '''
        Add item to playlist.

        playlist_index: integer [Required]
            ID of playlist
        search: str [Required]
            The song to search and retrieve from youtube.
            This could be a simple search, an ID or URL.
        '''
        return await self.__playlist_item_add(ctx, playlist_index, search)

    async def __playlist_item_add(self, ctx, playlist_index, search):
        if not await self.check_user_role(ctx):
            return await retry_discord_message_command(ctx.send, 'Unable to verify user role, ignoring command', delete_after=self.delete_after)
        if not await self.__check_author_voice_chat(ctx):
            return
        if not await self.__check_database_session(ctx):
            return retry_discord_message_command(ctx.send, 'Database not set, cannot use playlist functions', delete_after=self.delete_after)

        vc = ctx.voice_client

        if not vc:
            await ctx.invoke(self.connect_)
            vc = ctx.voice_client

        player = await self.get_player(ctx, vc.channel)

        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None

        source_entries = await player.download_client.check_source(search, ctx.guild.id, ctx.author.name, self.bot.loop)
        for entry in source_entries:
            source = await player.download_client.create_source(entry, self.bot.loop, download=False)
            if source is None:
                await retry_discord_message_command(ctx.send, f'Unable to find video for search {search}')
                continue
            self.logger.info(f'Music :: Adding video_id {source["id"]} to playlist "{playlist.name}" '
                             f' in guild {ctx.guild.id}')
            try:
                playlist_item = self.__playlist_add_item(playlist, source['id'], source['title'], source['uploader'])
            except PlaylistMaxLength:
                retry_discord_message_command(ctx.send, f'Cannot add more items to playlist "{playlist.name}", already max size', delete_after=self.delete_after)
                return
            if playlist_item:
                await retry_discord_message_command(ctx.send, f'Added item "{source["title"]}" to playlist {playlist_index}', delete_after=self.delete_after)
                continue
            await retry_discord_message_command(ctx.send, 'Unable to add playlist item, likely already exists', delete_after=self.delete_after)

    @playlist.command(name='item-search')
    async def playlist_item_search(self, ctx, playlist_index, *, search: str):
        '''
        Find item indexes in playlist that match search

        playlist_index: integer [Required]
            ID of playlist
        search: str [Required]
            String to look for in item title
        '''
        if not await self.check_user_role(ctx):
            return await retry_discord_message_command(ctx.send, 'Unable to verify user role, ignoring command',
                                            delete_after=self.delete_after)
        if not await self.__check_author_voice_chat(ctx):
            return
        if not await self.__check_database_session(ctx):
            return retry_discord_message_command(ctx.send, 'Database not set, cannot use playlist functions',
                                      delete_after=self.delete_after)

        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None

        query = self.db_session.query(PlaylistItem).\
            filter(PlaylistItem.playlist_id == playlist.id)
        items = []
        for (count, item) in enumerate(query):
            if search.lower() in item.title.lower():
                items.append({
                    'count': count + 1,
                    'title': item.title,
                })
        if not items:
            return await retry_discord_message_command(ctx.send, f'No playlist items in matching string "{search}"',
                                            delete_after=self.delete_after)

        headers = [
            {
                'name': 'ID',
                'length': 3,
            },
            {
                'name': 'Title',
                'length': 64,
            },
        ]
        table = DapperTable(headers, rows_per_message=15)
        for (count, item) in enumerate(items):
            table.add_row([
                item['count'],
                item['title'],
            ])
        messages = [f'```{t}```' for t in table.print()]
        for mess in messages:
            await retry_discord_message_command(ctx.send, mess, delete_after=self.delete_after)

    @playlist.command(name='item-remove')
    async def playlist_item_remove(self, ctx, playlist_index, song_index):
        '''
        Add item to playlist

        playlist_index: integer [Required]
            ID of playlist
        song_index: integer [Required]
            ID of song to remove
        '''
        if not await self.check_user_role(ctx):
            return await retry_discord_message_command(ctx.send, 'Unable to verify user role, ignoring command', delete_after=self.delete_after)
        if not await self.__check_author_voice_chat(ctx):
            return
        if not await self.__check_database_session(ctx):
            return retry_discord_message_command(ctx.send, 'Database not set, cannot use playlist functions', delete_after=self.delete_after)

        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None
        try:
            song_index = int(song_index)
        except ValueError:
            return await retry_discord_message_command(ctx.send, f'Invalid item index {song_index}',
                                            delete_after=self.delete_after)
        if song_index < 1:
            return await retry_discord_message_command(ctx.send, f'Invalid item index {song_index}',
                                            delete_after=self.delete_after)

        query = self.db_session.query(PlaylistItem).\
            filter(PlaylistItem.playlist_id == playlist.id)
        query_results = [item for item in query]
        try:
            item = query_results[song_index - 1]
            self.db_session.delete(item)
            self.db_session.commit()
            return await retry_discord_message_command(ctx.send, f'Removed item {song_index} from playlist',
                                            delete_after=self.delete_after)
        except IndexError:
            return await retry_discord_message_command(ctx.send, f'Unable to find item {song_index}',
                                            delete_after=self.delete_after)

    @playlist.command(name='show')
    async def playlist_show(self, ctx, playlist_index):
        '''
        Show Items in playlist

        playlist_index: integer [Required]
            ID of playlist
        '''
        if not await self.check_user_role(ctx):
            return await retry_discord_message_command(ctx.send, 'Unable to verify user role, ignoring command', delete_after=self.delete_after)
        if not await self.__check_author_voice_chat(ctx):
            return
        if not await self.__check_database_session(ctx):
            return retry_discord_message_command(ctx.send, 'Database not set, cannot use playlist functions', delete_after=self.delete_after)

        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None

        query = self.db_session.query(PlaylistItem).\
            filter(PlaylistItem.playlist_id == playlist.id)
        headers = [
            {
                'name': 'Pos',
                'length': 3,
            },
            {
                'name': 'Title',
                'length': 64,
            },
            {
                'name': 'Uploader',
                'length': 32,
            },
        ]
        table = DapperTable(headers, rows_per_message=15)
        for (count, item) in enumerate(query): #pylint:disable=protected-access
            uploader = item.uploader or ''
            table.add_row([
                f'{count + 1}',
                item.title,
                uploader,
            ])
            # Backwards compat for new field
            if not item.created_at:
                item.created_at = datetime.utcnow()
                self.db_session.add(item)
                self.db_session.commit()
        messages = [f'```{t}```' for t in table.print()]
        for mess in messages:
            await retry_discord_message_command(ctx.send, mess, delete_after=self.delete_after)

    @playlist.command(name='delete')
    async def playlist_delete(self, ctx, playlist_index):
        '''
        Delete playlist

        playlist_index: integer [Required]
            ID of playlist
        '''
        return await self.__playlist_delete(ctx, playlist_index)

    async def __playlist_delete(self, ctx, playlist_index):
        if not await self.check_user_role(ctx):
            return await retry_discord_message_command(ctx.send, 'Unable to verify user role, ignoring command', delete_after=self.delete_after)
        if not await self.__check_author_voice_chat(ctx):
            return
        if not await self.__check_database_session(ctx):
            return retry_discord_message_command(ctx.send, 'Database not set, cannot use playlist functions', delete_after=self.delete_after)

        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None
        self.logger.info(f'Music :: Deleting playlist items "{playlist.name}"')
        self.db_session.query(PlaylistItem).\
            filter(PlaylistItem.playlist_id == playlist.id).delete()
        self.db_session.delete(playlist)
        self.db_session.commit()
        return await retry_discord_message_command(ctx.send, f'Deleted playlist {playlist_index}',
                                                   delete_after=self.delete_after)

    @playlist.command(name='rename')
    async def playlist_rename(self, ctx, playlist_index, *, playlist_name: str):
        '''
        Rename playlist to new name

        playlist_index: integer [Required]
            ID of playlist
        playlist_name: str [Required]
            New name of playlist
        '''
        if not await self.check_user_role(ctx):
            return await retry_discord_message_command(ctx.send, 'Unable to verify user role, ignoring command', delete_after=self.delete_after)
        if not await self.__check_author_voice_chat(ctx):
            return
        if not await self.__check_database_session(ctx):
            return retry_discord_message_command(ctx.send, 'Database not set, cannot use playlist functions', delete_after=self.delete_after)

        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None
        self.logger.info(f'Music :: Renaming playlist {playlist.id} to name "{playlist_name}"')
        playlist.name = playlist_name
        self.db_session.commit()
        return await retry_discord_message_command(ctx.send, f'Renamed playlist {playlist_index} to name "{playlist_name}"', delete_after=self.delete_after)

    @playlist.command(name='save-queue')
    async def playlist_queue_save(self, ctx, *, name: str):
        '''
        Save contents of queue to a new playlist

        name: str [Required]
            Name of new playlist to create
        '''
        return await self.__playlist_queue_save(ctx, name)

    @playlist.command(name='save-history')
    async def playlist_history_save(self, ctx, *, name: str):
        '''
        Save contents of history to a new playlist

        name: str [Required]
            Name of new playlist to create
        '''
        return await self.__playlist_queue_save(ctx, name, is_history=True)

    async def __playlist_queue_save(self, ctx, name, is_history=False):
        playlist = await self.__playlist_create(ctx, name)
        if not playlist:
            return None

        try:
            player = self.players[ctx.guild.id]
        except KeyError:
            return await retry_discord_message_command(ctx.send, 'No player connected, no queue to save',
                                                       delete_after=self.delete_after)
        # Do a deepcopy here so list doesn't mutate as we iterate
        if is_history:
            queue_copy = deepcopy(player.history._queue) #pylint:disable=protected-access
        else:
            queue_copy = deepcopy(player.play_queue._queue) #pylint:disable=protected-access

        self.logger.info(f'Music :: Saving queue contents to playlist "{name}", is_history? {is_history}')

        if len(queue_copy) == 0:
            return await retry_discord_message_command(ctx.send, 'There are no songs to add to playlist',
                                                       delete_after=self.delete_after)

        for data in queue_copy:
            try:
                playlist_item = self.__playlist_add_item(playlist, data['id'], data['title'], data['uploader'])
            except PlaylistMaxLength:
                retry_discord_message_command(ctx.send, f'Cannot add more items to playlist "{playlist.name}", already max size', delete_after=self.delete_after)
                break
            if playlist_item:
                await retry_discord_message_command(ctx.send, f'Added item "{data["title"]}" to playlist', delete_after=self.delete_after)
                continue
            await retry_discord_message_command(ctx.send, f'Unable to add playlist item "{data["title"]}", likely already exists', delete_after=self.delete_after)
        await retry_discord_message_command(ctx.send, f'Finished adding items to playlist "{name}"', delete_after=self.delete_after)
        if is_history:
            player.history.clear()
            await retry_discord_message_command(ctx.send, 'Cleared history', delete_after=self.delete_after)
        return

    @playlist.command(name='queue')
    async def playlist_queue(self, ctx, playlist_index, sub_command: Optional[str] = ''):
        '''
        Add playlist to queue

        playlist_index: integer [Required]
            ID of playlist
        Sub commands - [shuffle] [max_number]
            shuffle - Shuffle playlist when entering it into queue
            max_num - Only add this number of songs to the queue
        '''
        if not await self.check_user_role(ctx):
            return await retry_discord_message_command(ctx.send, 'Unable to verify user role, ignoring command', delete_after=self.delete_after)
        if not await self.__check_author_voice_chat(ctx):
            return
        if not await self.__check_database_session(ctx):
            return retry_discord_message_command(ctx.send, 'Database not set, cannot use playlist functions', delete_after=self.delete_after)
        # Make sure sub command is valid
        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None
        shuffle = False
        max_num = None
        if sub_command:
            if 'shuffle' in sub_command.lower():
                shuffle = True
            number_matcher = re_match(NUMBER_REGEX, sub_command.lower())
            if number_matcher:
                max_num = int(number_matcher.group('number'))
        return await self.__playlist_queue(ctx, playlist, shuffle, max_num)

    @commands.command(name='random-play')
    async def playlist_random_play(self, ctx, sub_command: Optional[str] = ''):
        '''
        Play random songs from history

        Sub commands - [max_num]
            max_num - Number of songs to add to the queue at maximum
        '''
        if not await self.check_user_role(ctx):
            return await retry_discord_message_command(ctx.send, 'Unable to verify user role, ignoring command', delete_after=self.delete_after)
        if not await self.__check_author_voice_chat(ctx):
            return
        if not await self.__check_database_session(ctx):
            return retry_discord_message_command(ctx.send, 'Database not set, cannot use playlist functions', delete_after=self.delete_after)
        max_num = DEFAULT_RANDOM_QUEUE_LENGTH
        if sub_command:
            try:
                max_num = int(sub_command)
            except ValueError:
                retry_discord_message_command(ctx.send, f'Using default number of max songs {DEFAULT_RANDOM_QUEUE_LENGTH}', delete_after=self.delete_after)
        history_playlist = self.db_session.query(Playlist).\
            filter(Playlist.server_id == str(ctx.guild.id)).\
            filter(Playlist.is_history == True).first()

        if not history_playlist:
            return await retry_discord_message_command(ctx.send, 'Unable to find history for server', delete_after=self.delete_after)
        return await self.__playlist_queue(ctx, history_playlist, True, max_num, is_history=True)

    async def __playlist_queue(self, ctx, playlist, shuffle, max_num, is_history=False):
        vc = ctx.voice_client
        if not vc:
            await ctx.invoke(self.connect_)
            vc = ctx.voice_client
        player = await self.get_player(ctx, vc.channel)

        self.logger.info(f'Music :: Playlist queue called for playlist "{playlist.name}" in server "{ctx.guild.id}"')
        query = self.db_session.query(PlaylistItem).\
            filter(PlaylistItem.playlist_id == playlist.id)
        playlist_items = []
        # Backwards compat for new field
        for item in query:
            playlist_items.append(item)
            if not item.created_at:
                item.created_at = datetime.utcnow()
                self.db_session.add(item)
                self.db_session.commit()

        if shuffle:
            await retry_discord_message_command(ctx.send, 'Shuffling playlist items',
                                                delete_after=self.delete_after)
            random_shuffle(playlist_items)

        if max_num:
            if max_num < 0:
                await retry_discord_message_command(ctx.send, f'Invalid number of songs {max_num}',
                                                    delete_after=self.delete_after)
                return
            if max_num < len(playlist_items):
                playlist_items = playlist_items[:max_num]
            else:
                max_num = 0

        broke_early = False
        for item in playlist_items:
            message = await retry_discord_message_command(ctx.send, f'Downloading and processing "{item.title}"')
            try:
                # Just add directly to download queue here, since we already know the video id
                player.download_queue.put_nowait({
                    'search_string': item.video_id,
                    'guild_id': ctx.guild.id,
                    'requester': ctx.author.name,
                    'message': message,
                    # Pass history so we know to pass into history check later
                    'added_from_history': is_history,
                })
            except QueueFull:
                await retry_discord_message_command(ctx.send, f'Unable to add item "{item.title}" with id "{item.video_id}" to queue, queue is full',
                                                    delete_after=self.delete_after)
                await retry_discord_message_command(message.delete)
                broke_early = True
                break
        if broke_early:
            await retry_discord_message_command(ctx.send, f'Added as many songs in playlist "{playlist.name}" to queue as possible, but hit limit',
                                     delete_after=self.delete_after)
        elif max_num:
            await retry_discord_message_command(ctx.send, f'Added {max_num} songs from "{playlist.name}" to queue',
                                                delete_after=self.delete_after)
        else:
            await retry_discord_message_command(ctx.send, f'Added all songs in playlist "{playlist.name}" to queue',
                                                delete_after=self.delete_after)
        playlist.last_queued = datetime.utcnow()
        self.db_session.commit()

    @playlist.command(name='cleanup')
    async def playlist_cleanup(self, ctx, playlist_index): #pylint:disable=too-many-branches
        '''
        Remove items in playlist where we cannot find source

        playlist_index: integer [Required]
            ID of playlist
        '''
        if not await self.check_user_role(ctx):
            return await retry_discord_message_command(ctx.send, 'Unable to verify user role, ignoring command', delete_after=self.delete_after)
        if not await self.__check_author_voice_chat(ctx):
            return
        if not await self.__check_database_session(ctx):
            return retry_discord_message_command(ctx.send, 'Database not set, cannot use playlist functions', delete_after=self.delete_after)

        vc = ctx.voice_client

        if not vc:
            await ctx.invoke(self.connect_)
            vc = ctx.voice_client

        player = await self.get_player(ctx, vc.channel)

        self.logger.info(f'Music :: Playlist cleanup called on index "{playlist_index}" in server "{ctx.guild.id}"')
        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None

        for item in self.db_session.query(PlaylistItem).filter(PlaylistItem.playlist_id == playlist.id):
            # Check directly against create source here
            # Since we know the video id already
            entry = {
                'search_string': item.video_id,
                'guild_id': ctx.guild.id,
                'requester': ctx.author.name,
            }
            source = await player.download_client.create_source(entry, self.bot.loop, download=False)
            if source is None:
                self.logger.info(f'Music :: Unable to find source for "{item.title}", removing from database')
                await retry_discord_message_command(ctx.send, f'Unable to find youtube source ' \
                                         f'for "{item.title}", "{item.video_id}", removing item from database',
                                         delete_after=self.delete_after)
                self.db_session.delete(item)
                self.db_session.commit()
        self.logger.info(f'Music :: Finished cleanup for all items in playlist "{playlist.id}"')
        await retry_discord_message_command(ctx.send, f'Checked all songs in playlist "{playlist.name}"',
                                 delete_after=self.delete_after)

    @playlist.command(name='merge')
    async def playlist_merge(self, ctx, playlist_index_one, playlist_index_two):
        '''
        Merge second playlist into first playlist, deletes second playlist

        playlist_index_one: integer [Required]
            ID of playlist to be merged, will be kept
        playlist_index_two: integer [Required]
            ID of playlist to be merged, will be deleted
        '''
        if not await self.check_user_role(ctx):
            return await retry_discord_message_command(ctx.send, 'Unable to verify user role, ignoring command', delete_after=self.delete_after)
        if not await self.__check_author_voice_chat(ctx):
            return
        if not await self.__check_database_session(ctx):
            return retry_discord_message_command(ctx.send, 'Database not set, cannot use playlist functions', delete_after=self.delete_after)

        self.logger.info(f'Music :: Calling playlist merge of "{playlist_index_one}" and "{playlist_index_two}" in server "{ctx.guild.id}"')
        playlist_one = await self.__get_playlist(playlist_index_one, ctx)
        playlist_two = await self.__get_playlist(playlist_index_two, ctx)
        if not playlist_one:
            return retry_discord_message_command(ctx.send, f'Cannot find playlist {playlist_index_one}', delete_after=self.delete_after)
        if not playlist_two:
            return retry_discord_message_command(ctx.send, f'Cannot find playlist {playlist_index_two}', delete_after=self.delete_after)
        query = self.db_session.query(PlaylistItem).filter(PlaylistItem.playlist_id == playlist_two.id)
        for item in query:
            try:
                playlist_item = self.__playlist_add_item(playlist_one, item.video_id, item.title, item.uploader)
            except PlaylistMaxLength:
                retry_discord_message_command(ctx.send, f'Cannot add more items to playlist "{playlist_one.name}", already max size', delete_after=self.delete_after)
                return
            if playlist_item:
                await retry_discord_message_command(ctx.send, f'Added item "{item.title}" to playlist {playlist_index_one}', delete_after=self.delete_after)
                continue
            await retry_discord_message_command(ctx.send, f'Unable to add playlist item "{item.title}", likely already exists', delete_after=self.delete_after)
        await self.__playlist_delete(ctx, playlist_index_two)
