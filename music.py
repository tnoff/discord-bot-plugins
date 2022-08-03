import asyncio
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
import random
from re import match as re_match
from tempfile import NamedTemporaryFile, TemporaryDirectory
import typing
from uuid import uuid4

from async_timeout import timeout
from discord import FFmpegPCMAudio
from discord.errors import NotFound
from discord.ext import commands
from moviepy.editor import AudioFileClip, afx
from requests import get as requests_get
from requests import post as requests_post
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.exc import IntegrityError
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError

from discord_bot.cogs.common import CogHelper
from discord_bot.database import BASE

# Max title length for table views
MAX_STRING_LENGTH = 32

# Music defaults
DELETE_AFTER_DEFAULT = 300

# Max queue size
QUEUE_MAX_SIZE_DEFAULT = 128

# Max song length
MAX_SONG_LENGTH_DEFAULT = 60 * 15

# Spotify
SPOTIFY_AUTH_URL = 'https://accounts.spotify.com/api/token'
SPOTIFY_BASE_URL = 'https://api.spotify.com/v1/'
SPOTIFY_PLAYLIST_REGEX = r'^https://open.spotify.com/playlist/(?P<playlist_id>([a-zA-Z0-9]+))(?P<extra_query>(\?[a-zA-Z0-9=&_-]+)?)(?P<shuffle>( shuffle)?)'
SPOTIFY_ALBUM_REGEX = r'^https://open.spotify.com/album/(?P<album_id>([a-zA-Z0-9]+))(?P<extra_query>(\?[a-zA-Z0-9=&_-]+)?)(?P<shuffle>( shuffle)?)'

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
        })
        if auth_response.status_code != 200:
            raise Exception(f'Error getting auth token {auth_response.status_code}, {auth_response.text}')
        data = auth_response.json()
        self._token = data['access_token']
        self._expiry = datetime.now() + timedelta(seconds=data['expires_in'])

    def __gather_track_info(self, first_url):
        results = []
        url = first_url
        while True:
            r = requests_get(url, headers={'Authorization': f'Bearer {self.token}'})
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


# Music bot setup
# Music taken from https://gist.github.com/EvieePy/ab667b74e9758433b3eb806c53a19f34
class MyQueue(asyncio.Queue):
    '''
    Custom implementation of asyncio Queue
    '''
    def shuffle(self):
        '''
        Shuffle queue
        '''
        random.shuffle(self._queue)
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


def clean_string(stringy, max_length=MAX_STRING_LENGTH):
    '''
    Make sure string is not longer than max string
    '''
    if len(stringy) > max_length:
        stringy = f'{stringy[0:max_length-3]}...'
    return stringy

def get_table_view(items, max_rows=15, show_queue_time=False):
    '''
    Common function for queue printing
    max_rows    :   Only show max rows in a single print
    show_queue_time : Show time until item is played
    '''
    current_index = 0
    table_strings = []

    duration = 0
    # Assume first column is short index name
    # Second column is longer title name
    while True:
        table = ''
        for (count, item) in enumerate(items[current_index:]):
            uploader = item['uploader'].replace(' - Topic', '')
            table = f'{table}\n{count + current_index + 1:3} ||'
            if show_queue_time:
                delta = timedelta(seconds=duration)
                table = f'{table} {str(delta):10} ||'
                duration += item['duration']
            table = f'{table} {item["title"]:48} || {uploader:32}'
            if count >= max_rows - 1:
                break
        table_strings.append(f'```\n{table}\n```')
        current_index += max_rows
        if current_index >= len(items):
            break
    return table_strings

def get_queue_message(queue):
    '''
    Get full queue message
    '''
    items = []
    if not queue._queue: #pylint:disable=protected-access
        return None
    for item in queue._queue: #pylint:disable=protected-access
        uploader = ''
        if item['uploader'] is not None:
            uploader = clean_string(item['uploader'], max_length=32)
        items.append({
            'title': clean_string(item['title'], max_length=48),
            'uploader': uploader,
            'duration': item['duration'],
        })

    table_strings = get_table_view(items, show_queue_time=True)
    header = f'```{"Pos":3} || {"Queue Time":10} || {"Title":48} || {"Uploader":32}```'
    return [header] + table_strings

def get_finished_path(file_path):
    '''
    Get 'finished path' for edited file
    '''
    return file_path.parent / (file_path.stem + '.finished.mp3')

def get_editing_path(file_path):
    '''
    Get 'editing path' for editing files
    '''
    return file_path.parent / (file_path.stem + '.edited.mp3')

def remove_file_path(file_path):
    '''
    If file path exists, remove, but check if still being edited just in case
    '''
    if file_path.exists():
        finished_path = get_finished_path(file_path)
        if finished_path.exists():
            finished_path.unlink()
        if not get_editing_path(file_path).exists():
            file_path.unlink()

def edit_audio_file(file_path):
    '''
    Normalize audio for file
    '''
    finished_path = get_finished_path(file_path)
    editing_path = get_editing_path(file_path)
    try:
        edited_audio = AudioFileClip(str(file_path)).fx(afx.audio_normalize) #pylint:disable=no-member
        edited_audio.write_audiofile(str(editing_path))
        editing_path.rename(finished_path)
        return finished_path
    except OSError:
        # File likely was deleted in middle
        return None

class DownloadClient():
    '''
    Download Client using yt-dlp
    '''
    def __init__(self, ytdl, logger, spotify_client=None, delete_after=None, enable_audio_processing=False):
        self.ytdl = ytdl
        self.logger = logger
        self.spotify_client = spotify_client
        self.delete_after = delete_after
        self.enable_audio_processing = enable_audio_processing

    def __getitem__(self, item: str):
        '''
        Allows us to access attributes similar to a dict.

        This is only useful when you are NOT downloading.
        '''
        return self.__getattribute__(item)

    async def create_source(self, source_dict, loop):
        '''
        Download data from youtube search
        '''
        to_run = partial(self.__prepare_data_source, source_dict=source_dict)
        return await loop.run_in_executor(None, to_run)

    async def check_source(self, ctx, search, loop, max_song_length=None, download_queue=None):
        '''
        Create source from youtube search
        '''
        # If spotify, grab list of search strings, otherwise just grab single search
        spotify_playlist_matcher = re_match(SPOTIFY_PLAYLIST_REGEX, search)
        spotify_album_matcher = re_match(SPOTIFY_ALBUM_REGEX, search)

        if spotify_playlist_matcher and self.spotify_client:
            to_run = partial(self.__check_spotify_source, playlist_id=spotify_playlist_matcher.group('playlist_id'))
            search_strings = await loop.run_in_executor(None, to_run)
            if spotify_playlist_matcher.group('shuffle'):
                random.shuffle(search_strings)
            self.logger.debug(f'Gathered {len(search_strings)} from spotify playlist "{search}"')

        elif spotify_album_matcher and self.spotify_client:
            to_run = partial(self.__check_spotify_source, album_id=spotify_album_matcher.group('album_id'))
            search_strings = await loop.run_in_executor(None, to_run)
            if spotify_album_matcher.group('shuffle'):
                random.shuffle(search_strings)
            self.logger.debug(f'Gathered {len(search_strings)} from spotify playlist "{search}"')

        else:
            search_strings = [search]

        all_entries = []
        for search_string in search_strings:
            to_run = partial(self.__run_search, search_string=search_string)
            data_entries = await loop.run_in_executor(None, to_run)
            if data_entries is None:
                return await ctx.send(f'Unable to find youtube source for "{search_string}"',
                                    delete_after=self.delete_after)

            for entry in data_entries:
                if max_song_length and entry['duration'] > max_song_length:
                    await ctx.send(f'Unable to add "<{entry["webpage_url"]}>" to queue, exceeded max length '
                                   f'{max_song_length} seconds', delete_after=self.delete_after)
                    continue
                entry['guild_id'] = ctx.guild.id
                entry['requester'] = f'{ctx.author.name}'
                if not download_queue:
                    all_entries.append(entry)
                else:
                    try:
                        self.logger.debug(f'Handing off song song "{entry["title"]}" with url "{entry["webpage_url"]}" to download queue')
                        download_queue.put_nowait(entry)
                    except asyncio.QueueFull:
                        # Return here since we probably cant find any more entries
                        self.logger.warning(f'Queue too full, skipping song "{entry["title"]}"')
                        await ctx.send('Queue is full, cannot add more songs',
                                       delete_after=self.delete_after)
                        return all_entries
            # Wait 1 second so we dont blast youtube
            await asyncio.sleep(1)
        return all_entries

    def __run_search(self, search_string):
        try:
            entry_data = self.ytdl.extract_info(url=search_string, download=False)
        except DownloadError:
            self.logger.error(f'Error downloading youtube search {search_string}')
            return None
        try:
            entries = entry_data['entries']
        except KeyError:
            # Sometimes direct search returns a single item
            if 'id' in entry_data:
                return [entry_data]
            return None
        if not entries:
            self.logger.error(f'No entries found for {search_string}')
            return None
        return entries

    def __check_spotify_source(self, playlist_id=None, album_id=None):
        data = []
        if playlist_id:
            self.logger.debug(f'Checking for spotify playlist {playlist_id}')
            response, data = self.spotify_client.playlist_get(playlist_id)
            if response.status_code != 200:
                self.logger.error(f'Unable to find spotify data {response.status_code}, {response.text}')
                return []
        if album_id:
            self.logger.debug(f'Checking for spotify album {album_id}')
            response, data = self.spotify_client.album_get(album_id)
            if response.status_code != 200:
                self.logger.error(f'Unable to find spotify data {response.status_code}, {response.text}')
                return []
        search_strings = []
        for item in data:
            search_string = f'{item["track_name"]} {item["track_artists"]}'
            search_strings.append(search_string)
        return search_strings

    def __prepare_data_source(self, source_dict):
        '''
        Prepare source from youtube url
        '''
        try:
            data = self.ytdl.extract_info(url=source_dict['webpage_url'], download=True)
        except DownloadError:
            self.logger.error(f'Error downloading youtube search "{source_dict["webpage_url"]}')
            return None
        self.logger.info(f'Starting download of video "{data["title"]}" from url "{data["webpage_url"]}"')
        file_path = Path(self.ytdl.prepare_filename(data))
        # The modified time of download videos can be the time when it was actually uploaded to youtube
        # Touch here to update the modified time, so that the cleanup check works as intendend
        file_path.touch(exist_ok=True)
        # Rename file to a random uuid name, that way we can have diff videos with same/similar names
        uuid_path = file_path.parent / f'{uuid4()}{".".join(i for i in file_path.suffixes)}'
        file_path.rename(uuid_path)
        self.logger.info(f'Downloaded url "{data["webpage_url"]} to file "{uuid_path}"')
        data['requester'] = source_dict['requester']
        data['guild_id'] = source_dict['guild_id']
        data['file_path'] = uuid_path
        if self.enable_audio_processing:
            edited_path = edit_audio_file(uuid_path)
            if edited_path:
                data['file_path'] = edited_path
                uuid_path.unlink()
        return data


class MusicPlayer:
    '''
    A class which is assigned to each guild using the bot for Music.

    This class implements a queue and loop, which allows for different guilds
    to listen to different playlists simultaneously.

    When the bot disconnects from the Voice it's instance will be destroyed.
    '''

    def __init__(self, ctx, logger, ytdl, max_song_length, queue_max_size, delete_after):
        self.bot = ctx.bot
        self.logger = logger
        self._guild = ctx.guild
        self._channel = ctx.channel
        self._cog = ctx.cog
        self.ytdl = ytdl
        self.delete_after = delete_after

        self.logger.info(f'Max length for music queue in guild {self._guild} is {queue_max_size}')
        self.download_queue = MyQueue(maxsize=queue_max_size)
        self.play_queue = MyQueue(maxsize=queue_max_size)
        self.history = MyQueue(maxsize=queue_max_size)
        self.next = asyncio.Event()

        self.np = None  # Now playing message
        self.queue_messages = [] # Show current queue
        self.volume = 1
        self.max_song_length = max_song_length
        self.current_path = None

        # For showing messages
        self.lock_file = Path(NamedTemporaryFile(delete=False).name) #pylint:disable=consider-using-with

        ctx.bot.loop.create_task(self.player_loop())
        ctx.bot.loop.create_task(self.download_files())

    async def acquire_lock(self, wait_timeout=600):
        '''
        Wait for and acquire lock
        '''
        start = datetime.now()
        while True:
            if (datetime.now() - start).seconds > wait_timeout:
                raise Exception('Error acquiring markov lock')
            if self.lock_file.read_text() == 'locked':
                await asyncio.sleep(.5)
                continue
            break
        self.lock_file.write_text('locked')

    async def release_lock(self):
        '''
        Release lock
        '''
        self.lock_file.write_text('unlocked')

    async def check_latest_message(self, last_message_id):
        '''
        Check if latest message in history is no longer queue message sent
        '''
        history = await self._channel.history(limit=1).flatten()
        return history[0].id == last_message_id

    async def clear_queue_messages(self):
        '''
        Delete queue messages
        '''
        await self.acquire_lock()
        for queue_message in self.queue_messages:
            await queue_message.delete()
        self.queue_messages = []
        await self.release_lock()

    async def update_queue_strings(self, delete_messages=False):
        '''
        Update queue message in channel
        '''
        await self.acquire_lock()
        self.logger.debug(f'Updating queue messages in channel {self._channel.id}')
        new_queue_strings = get_queue_message(self.play_queue) or []
        if delete_messages or len(self.queue_messages) != len(new_queue_strings):
            for queue_message in self.queue_messages:
                await queue_message.delete()
            self.queue_messages = []
            for table in new_queue_strings:
                self.queue_messages.append(await self._channel.send(table))
        else:
            # Can skip first message as its likely just column names
            for (count, queue_message) in enumerate(self.queue_messages):
                await queue_message.edit(content=new_queue_strings[count])
        await self.release_lock()

    async def download_files(self):
        '''
        Go through download loop and download all files
        '''
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():
            source_dict = await self.download_queue.get()
            try:
                source_download = await self.ytdl.create_source(source_dict, self.bot.loop)
                self.play_queue.put_nowait(source_download)
                self.logger.info(f'Adding "{source_download["title"]}" '
                                 f'to queue in guild {source_dict["guild_id"]}')
            except asyncio.QueueFull:
                await self._channel.send('Queue is full, cannot add more songs',
                                         delete_after=self.delete_after)
                continue
            await self.update_queue_strings()
            # Wait 1 second so we dont blast youtube api
            await asyncio.sleep(1)

    async def __reset_now_playing_message(self, message):
        last_message_check = None
        if self.queue_messages:
            last_message_check = await self.check_latest_message(self.queue_messages[-1])
        # Double check np message exists
        if self.np:
            try:
                await self._channel.fetch_message(self.np.id)
            except NotFound:
                self.np = None
        # If not exists, send
        if self.np is None:
            self.np = await self._channel.send(message)
            return True
        # If message after existing queue, print
        if not last_message_check:
            await self.np.delete()
            self.np = await self._channel.send(message)
            return True
        await self.np.edit(content=message)
        return False


    async def player_loop(self):
        '''
        Our main player loop.
        '''
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():
            self.next.clear()

            try:
                # Wait for the next song. If we timeout cancel the player and disconnect...
                async with timeout(self.max_song_length + 60): # Max song length + 1 min
                    source_dict = await self.play_queue.get()
            except asyncio.TimeoutError:
                self.logger.error(f'Music bot reached timeout on queue in guild "{self._guild}"')
                return await self.destroy(self._guild)

            self.current_path = source_dict['file_path']
            # Check if edited "finished" file exists
            finished_path = get_finished_path(source_dict['file_path'])
            if finished_path.exists():
                source_dict['file_path'].unlink()
                source_dict['file_path'] = finished_path

            # Double check file didnt go away
            if not source_dict['file_path'].exists():
                await self._channel.send(f'Unable to play "{source_dict["title"]}", local file dissapeared')
                continue

            source = FFmpegPCMAudio(str(source_dict['file_path']))

            source.volume = self.volume
            try:
                self._guild.voice_client.play(source, after=lambda _: self.bot.loop.call_soon_threadsafe(self.next.set)) #pylint:disable=line-too-long
            except AttributeError:
                self.logger.info(f'No voice client found, disconnecting from guild {self._guild}')
                return await self.destroy(self._guild)
            self.logger.info(f'Music bot now playing "{source_dict["title"]}" requested '
                             f'by "{source_dict["requester"]}" in guild "{self._guild}", url '
                             f'"{source_dict["webpage_url"]}"')
            message = f'Now playing {source_dict["webpage_url"]} requested by {source_dict["requester"]}'
            delete_messages = await self.__reset_now_playing_message(message)

            await asyncio.sleep(1)
            await self.update_queue_strings(delete_messages=delete_messages)

            await self.next.wait()

            # Make sure the FFmpeg process is cleaned up.
            source.cleanup()
            if source_dict['file_path'].exists():
                source_dict['file_path'].unlink()

            try:
                self.history.put_nowait(source_dict)
            except asyncio.QueueFull:
                await self.history.get()
                self.history.put_nowait(source_dict)

            if self.play_queue.empty() and self.download_queue.empty():
                await self.np.delete()
                self.np = None

    async def clear_remaining_queue(self):
        '''
        Delete files downloaded for queue
        '''
        while True:
            # Clear download queue
            try:
               # 2 seconds here is kind of random time, but wait if any leftover downloads happening
                async with timeout(2):
                    await self.download_queue.get()
            except asyncio.TimeoutError:
                break
            # Clear player queue
            try:
               # 2 seconds here is kind of random time, but wait if any leftover downloads happening
                async with timeout(2):
                    source_dict = await self.play_queue.get()
                    if source_dict['file_path'].exists():
                        source_dict['file_path'].unlink()
            except asyncio.TimeoutError:
                break

    async def destroy(self, guild):
        '''
        Disconnect and cleanup the player.
        '''
        self.logger.info(f'Removing music bot from guild {self._guild}')
        self.bot.loop.create_task(self._cog.cleanup(guild))


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
        self.max_song_length = settings.get('music_max_song_length', MAX_SONG_LENGTH_DEFAULT)
        self.download_dir = settings.get('music_download_dir', None)
        self.enable_audio_processing = settings.get('music_enable_audio_processing', False)
        spotify_client_id = settings.get('music_spotify_client_id', None)
        spotify_client_secret = settings.get('music_spotify_client_secret', None)
        if spotify_client_id and spotify_client_secret:
            self.spotify_client = SpotifyClient(spotify_client_id, spotify_client_secret)

        if self.download_dir is not None:
            self.download_dir = Path(self.download_dir)
            if not self.download_dir.exists():
                self.download_dir.mkdir(exist_ok=True, parents=True)
        else:
            self.download_dir = Path(TemporaryDirectory().name) #pylint:disable=consider-using-with

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

        try:
            if player.np:
                await player.np.delete()
        except NotFound:
            pass
        await player.clear_queue_messages()
        await player.clear_remaining_queue()
        del self.players[guild.id]

    async def __check_database_session(self, ctx):
        '''
        Check if database session is in use
        '''
        if not self.db_session:
            await ctx.send('Functionality not available, database is not enabled')
            return False
        return True

    def get_player(self, ctx):
        '''
        Retrieve the guild player, or generate one.
        '''
        try:
            player = self.players[ctx.guild.id]
        except KeyError:
            guild_path = self.download_dir / f'{ctx.guild.id}'
            guild_path.mkdir(exist_ok=True, parents=True)
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
                'outtmpl': str(guild_path / '%(extractor)s-%(id)s-%(title)s.%(ext)s'),
            }
            ytdl = DownloadClient(YoutubeDL(ytdlopts), self.logger,
                                  spotify_client=self.spotify_client, delete_after=self.delete_after,
                                  enable_audio_processing=self.enable_audio_processing)
            player = MusicPlayer(ctx, self.logger, ytdl, self.max_song_length,
                                 self.queue_max_size, self.delete_after)
            self.players[ctx.guild.id] = player

        return player

    @commands.command(name='join', aliases=['awaken'])
    async def connect_(self, ctx):
        '''
        Connect to voice channel.
        '''
        return await self.retry_command(self.__connect, ctx)

    async def __connect(self, ctx):
        try:
            channel = ctx.author.voice.channel
        except AttributeError:
            return await ctx.send('No channel to join. Please either '
                                    'specify a valid channel or join one.', delete_after=self.delete_after)
        vc = ctx.voice_client

        if vc:
            if vc.channel.id == channel.id:
                return
            try:
                self.logger.info(f'Music bot moving to channel {channel.id} '
                                 f'in guild {ctx.guild.id}')
                await vc.move_to(channel)
            except asyncio.TimeoutError:
                self.logger.error(f'Moving to channel {channel.id} timed out')
                return await ctx.send(f'Moving to channel: <{channel}> timed out.')
        else:
            try:
                await channel.connect()
            except asyncio.TimeoutError:
                self.logger.error(f'Connecting to channel {channel.id} timed out')
                return await ctx.send(f'Connecting to channel: <{channel}> timed out.')

        await ctx.send(f'Connected to: {channel}', delete_after=self.delete_after)

    @commands.command(name='play')
    async def play_(self, ctx, *, search: str):
        '''
        Request a song and add it to the queue.

        search: str [Required]
            The song to search and retrieve from youtube.
            This could be a simple search, an ID or URL.
        '''
        return await self.retry_command(self.__play, ctx, search)

    async def __play(self, ctx, search):
        await ctx.trigger_typing()

        vc = ctx.voice_client

        if not vc:
            await ctx.invoke(self.connect_)

        player = self.get_player(ctx)

        if player.play_queue.full():
            return await ctx.send('Queue is full, cannot add more songs',
                                  delete_after=self.delete_after)

        await player.ytdl.check_source(ctx, search, self.bot.loop,
                                       max_song_length=self.max_song_length, download_queue=player.download_queue)

    @commands.command(name='skip')
    async def skip_(self, ctx):
        '''
        Skip the song.
        '''
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await ctx.send('I am not currently playing anything',
                                  delete_after=self.delete_after)

        if not vc.is_paused() and not vc.is_playing():
            return
        vc.stop()
        await ctx.send('Skipping song',
                       delete_after=self.delete_after)

    @commands.command(name='clear')
    async def clear(self, ctx):
        '''
        Clear all items from queue
        '''
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await ctx.send('I am not currently playing anything',
                                  delete_after=self.delete_after)

        player = self.get_player(ctx)
        if player.play_queue.empty():
            return await ctx.send('There are currently no more queued songs.',
                                  delete_after=self.delete_after)
        player.play_queue.clear()
        await ctx.send('Cleared all items from queue',
                       delete_after=self.delete_after)

        # Reset queue messages
        await player.clear_queue_messages()
        await player.clear_remaining_queue()

    @commands.command(name='history')
    async def history_(self, ctx):
        '''
        Show recently played songs
        '''
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await ctx.send('I am not currently playing anything',
                                  delete_after=self.delete_after)

        player = self.get_player(ctx)
        if player.history.empty():
            return await ctx.send('There have been no songs played.',
                                  delete_after=self.delete_after)

        items = []
        for item in player.history._queue: #pylint:disable=protected-access
            uploader = ''
            if item['uploader'] is not None:
                uploader = clean_string(item['uploader'], max_length=32)
            items.append({
                'title': clean_string(item['title']),
                'uploader': clean_string(uploader),
            })

        tables = get_table_view(items)
        header = f'```{"Pos":3} || {"Title":48} || {"Uploader":32}```'
        await ctx.send(header, delete_after=self.delete_after)
        for table in tables:
            await ctx.send(table, delete_after=self.delete_after)

    @commands.command(name='shuffle')
    async def shuffle_(self, ctx):
        '''
        Shuffle song queue.
        '''
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await ctx.send('I am not currently playing anything',
                                  delete_after=self.delete_after)

        player = self.get_player(ctx)
        if player.play_queue.empty():
            return await ctx.send('There are currently no more queued songs.',
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
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await ctx.send('I am not currently connected to voice',
                                  delete_after=self.delete_after)

        player = self.get_player(ctx)
        if player.play_queue.empty():
            return await ctx.send('There are currently no more queued songs.',
                                  delete_after=self.delete_after)

        try:
            queue_index = int(queue_index)
        except ValueError:
            return await ctx.send(f'Invalid queue index {queue_index}',
                                  delete_after=self.delete_after)

        item = player.play_queue.remove_item(queue_index)
        if item is None:
            return ctx.send(f'Unable to remove queue index {queue_index}',
                            delete_after=self.delete_after)
        await ctx.send(f'Removed item {item["title"]} from queue',
                       delete_after=self.delete_after)
        remove_file_path(item['file_path'])
        await player.update_queue_strings()

    @commands.command(name='bump')
    async def bump_item(self, ctx, queue_index):
        '''
        Bump item to top of queue

        queue_index: integer [Required]
            Position in queue of song that will be removed.
        '''
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await ctx.send('I am not currently connected to voice',
                                  delete_after=self.delete_after)

        player = self.get_player(ctx)
        if player.play_queue.empty():
            return await ctx.send('There are currently no more queued songs.',
                                  delete_after=self.delete_after)

        try:
            queue_index = int(queue_index)
        except ValueError:
            return await ctx.send(f'Invalid queue index {queue_index}',
                                  delete_after=self.delete_after)

        item = player.play_queue.bump_item(queue_index)
        if item is None:
            return await ctx.send(f'Unable to bump queue index {queue_index}',
                            delete_after=self.delete_after)
        await ctx.send(f'Bumped item {item["title"]} to top of queue',
                       delete_after=self.delete_after)

        await player.update_queue_strings()

    @commands.command(name='stop')
    async def stop_(self, ctx):
        '''
        Stop the currently playing song and disconnect bot from voice chat.
        '''
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await ctx.send('I am not currently playing anything',
                                  delete_after=self.delete_after)

        await self.cleanup(ctx.guild)

    async def __get_playlist(self, playlist_index, ctx):
        try:
            index = int(playlist_index)
        except ValueError:
            await ctx.send(f'Invalid playlist index {playlist_index}', delete_after=self.delete_after)
            return None
        playlist_items = self.db_session.query(Playlist).\
            filter(Playlist.server_id == str(ctx.guild.id)).order_by(Playlist.created_at.asc())
        playlist_items = [p for p in playlist_items]

        if not playlist_items:
            await ctx.send('No playlists in database',
                           delete_after=self.delete_after)
            return None
        try:
            return playlist_items[index - 1]
        except IndexError:
            await ctx.send(f'Invalid playlist index {playlist_index}', delete_after=self.delete_after)
            return None

    @commands.group(name='playlist', invoke_without_command=False)
    async def playlist(self, ctx):
        '''
        Playlist functions.
        '''
        if ctx.invoked_subcommand is None:
            await ctx.send('Invalid sub command passed...', delete_after=self.delete_after)

    async def __playlist_create(self, ctx, name):
        if not await self.__check_database_session(ctx):
            ctx.send('Database not set, cannot use playlist functions', delete_after=self.delete_after)
            return None
        playlist = Playlist(name=clean_string(name, max_length=256),
                            server_id=ctx.guild.id,
                            created_at=datetime.utcnow())
        try:
            self.db_session.add(playlist)
            self.db_session.commit()
        except IntegrityError:
            self.db_session.rollback()
            await ctx.send(f'Unable to create playlist "{name}", name likely already exists')
            return None
        self.logger.info(f'Playlist created "{playlist.name}" with ID {playlist.id} in guild {ctx.guild.id}')
        await ctx.send(f'Created playlist "{name}"',
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
        return await self.retry_command(self.__playlist_list, ctx)

    async def __playlist_list(self, ctx, max_rows=15):
        if not await self.__check_database_session(ctx):
            return ctx.send('Database not set, cannot use playlist functions', delete_after=self.delete_after)
        playlist_items = self.db_session.query(Playlist).\
            filter(Playlist.server_id == str(ctx.guild.id))
        playlist_items = [p for p in playlist_items]

        if not playlist_items:
            return await ctx.send('No playlists in database',
                                  delete_after=self.delete_after)
        table_strings = []
        current_index = 0
        while True:
            table = ''
            for (count, item) in enumerate(playlist_items[current_index:]):
                last_queued = 'N/A'
                if item.last_queued:
                    last_queued = item.last_queued.strftime('%Y-%m-%d %H:%M:%S')
                table = f'{table}\n{count + current_index + 1:3} ||'
                table = f'{table} {clean_string(item.name, max_length=32):32} ||'
                table = f'{table} {last_queued:17}'
                if count >= max_rows - 1:
                    break
            table_strings.append(f'```\n{table}\n```')
            current_index += max_rows
            if current_index >= len(playlist_items):
                break
        await ctx.send(f'```{"ID":3} || {"Playlist Name":32} || {"Last Queued":17}```',
                       delete_after=self.delete_after)
        for table in table_strings:
            await ctx.send(f'{table}', delete_after=self.delete_after)

    def __playlist_add_item(self, ctx, playlist, data_id, data_title, data_uploader):
        self.logger.info(f'Adding video_id {data_id} to playlist "{playlist.name}" '
                         f' in guild {ctx.guild.id}')
        playlist_item = PlaylistItem(title=clean_string(data_title, max_length=256),
                                     video_id=data_id,
                                     uploader=clean_string(data_uploader, max_length=256),
                                     playlist_id=playlist.id)
        try:
            self.db_session.add(playlist_item)
            self.db_session.commit()
            return playlist_item
        except IntegrityError as e:
            self.logger.exception(e)
            self.logger.error(str(e))
            self.db_session.rollback()
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
        return await self.retry_command(self.__playlist_item_add, ctx, playlist_index, search)

    async def __playlist_item_add(self, ctx, playlist_index, search):
        if not await self.__check_database_session(ctx):
            return ctx.send('Database not set, cannot use playlist functions', delete_after=self.delete_after)

        vc = ctx.voice_client

        if not vc:
            await ctx.invoke(self.connect_)

        player = self.get_player(ctx)

        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None

        source_dicts = await player.ytdl.check_source(ctx, search, self.bot.loop)
        if source_dicts is None:
            return await ctx.send(f'Unable to find video for search {search}')
        for data in source_dicts:
            self.logger.info(f'Adding video_id {data["id"]} to playlist "{playlist.name}" '
                            f' in guild {ctx.guild.id}')
            playlist_item = self.__playlist_add_item(ctx, playlist, data['id'], data['title'], data['uploader'])
            if playlist_item:
                await ctx.send(f'Added item "{data["title"]}" to playlist {playlist_index}', delete_after=self.delete_after)
                continue
            await ctx.send('Unable to add playlist item, likely already exists', delete_after=self.delete_after)

    @playlist.command(name='item-search')
    async def playlist_item_search(self, ctx, playlist_index, *, search: str):
        '''
        Find item indexes in playlist that match search

        playlist_index: integer [Required]
            ID of playlist
        search: str [Required]
            String to look for in item title
        '''
        return await self.retry_command(self.__playlist_item_search, ctx, playlist_index, search)

    async def __playlist_item_search(self, ctx, playlist_index, search, max_rows=15):
        if not await self.__check_database_session(ctx):
            return ctx.send('Database not set, cannot use playlist functions', delete_after=self.delete_after)

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
            return await ctx.send(f'No playlist items in matching string "{search}"',
                                  delete_after=self.delete_after)

        table_strings = []
        current_index = 0
        while True:
            table = ''
            for (count, item) in enumerate(items[current_index:]):
                table = f'{table}\n{item["count"]:3} ||'
                table = f'{table} {clean_string(item["title"], max_length=48):32}'
                if count >= max_rows - 1:
                    break
            table_strings.append(f'```\n{table}\n```')
            current_index += max_rows
            if current_index >= len(items):
                break

        header = f'```{"ID":3} || {"Title":48}```'
        await ctx.send(header, delete_after=self.delete_after)
        for table in table_strings:
            await ctx.send(table, delete_after=self.delete_after)

    @playlist.command(name='item-remove')
    async def playlist_item_remove(self, ctx, playlist_index, song_index):
        '''
        Add item to playlist

        playlist_index: integer [Required]
            ID of playlist
        song_index: integer [Required]
            ID of song to remove
        '''
        return await self.retry_command(self.__playlist_item_remove, ctx, playlist_index, song_index)

    async def __playlist_item_remove(self, ctx, playlist_index, song_index):
        if not await self.__check_database_session(ctx):
            return ctx.send('Database not set, cannot use playlist functions', delete_after=self.delete_after)

        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None
        try:
            song_index = int(song_index)
        except ValueError:
            return await ctx.send(f'Invalid item index {song_index}',
                                  delete_after=self.delete_after)
        if song_index < 1:
            return await ctx.send(f'Invalid item index {song_index}',
                                  delete_after=self.delete_after)

        query = self.db_session.query(PlaylistItem).\
            filter(PlaylistItem.playlist_id == playlist.id)
        query_results = [item for item in query]
        try:
            item = query_results[song_index - 1]
            self.db_session.delete(item)
            self.db_session.commit()
            return await ctx.send(f'Removed item {song_index} from playlist',
                                  delete_after=self.delete_after)
        except IndexError:
            return await ctx.send(f'Unable to find item {song_index}',
                                  delete_after=self.delete_after)

    @playlist.command(name='show')
    async def playlist_show(self, ctx, playlist_index):
        '''
        Show Items in playlist

        playlist_index: integer [Required]
            ID of playlist
        '''
        return await self.retry_command(self.__playlist_show, ctx, playlist_index)

    async def __playlist_show(self, ctx, playlist_index):
        if not await self.__check_database_session(ctx):
            return ctx.send('Database not set, cannot use playlist functions', delete_after=self.delete_after)

        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None

        query = self.db_session.query(PlaylistItem).\
            filter(PlaylistItem.playlist_id == playlist.id)
        items = []
        for item in query:
            uploader = ''
            if item.uploader is not None:
                uploader = clean_string(item.uploader, max_length=32)
            items.append({
                'title': clean_string(item.title, max_length=48),
                'uploader': uploader,
            })
        if not items:
            return await ctx.send('No playlist items in database',
                                  delete_after=self.delete_after)

        tables = get_table_view(items)
        header = f'```{"Pos":3} || {"Title":48} || {"Uploader":32}```'
        await ctx.send(header, delete_after=self.delete_after)
        for table in tables:
            await ctx.send(table, delete_after=self.delete_after)

    @playlist.command(name='delete')
    async def playlist_delete(self, ctx, playlist_index):
        '''
        Delete playlist

        playlist_index: integer [Required]
            ID of playlist
        '''
        return await self.retry_command(self.__playlist_delete, ctx, playlist_index)

    async def __playlist_delete(self, ctx, playlist_index):
        if not await self.__check_database_session(ctx):
            return ctx.send('Database not set, cannot use playlist functions', delete_after=self.delete_after)

        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None
        self.logger.info(f'Deleting playlist items "{playlist.name}"')
        self.db_session.query(PlaylistItem).\
            filter(PlaylistItem.playlist_id == playlist.id).delete()
        self.db_session.delete(playlist)
        self.db_session.commit()
        return await ctx.send(f'Deleted playlist {playlist_index}',
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
        return await self.retry_command(self.__playlist_rename, ctx, playlist_index, playlist_name)

    async def __playlist_rename(self, ctx, playlist_index, playlist_name):
        if not await self.__check_database_session(ctx):
            return ctx.send('Database not set, cannot use playlist functions', delete_after=self.delete_after)

        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None
        self.logger.info(f'Renaming playlist {playlist.id} to name "{playlist_name}"')
        playlist.name = playlist_name
        self.db_session.commit()
        return await ctx.send(f'Renamed playlist {playlist_index} to name "{playlist_name}"', delete_after=self.delete_after)

    @playlist.command(name='save-queue')
    async def playlist_queue_save(self, ctx, *, name: str):
        '''
        Save contents of queue to a new playlist

        name: str [Required]
            Name of new playlist to create
        '''
        return await self.retry_command(self.__playlist_queue_save, ctx, name)

    @playlist.command(name='save-history')
    async def playlist_history_save(self, ctx, *, name: str):
        '''
        Save contents of history to a new playlist

        name: str [Required]
            Name of new playlist to create
        '''
        return await self.retry_command(self.__playlist_queue_save, ctx, name, is_history=True)

    async def __playlist_queue_save(self, ctx, name, is_history=False):
        playlist = await self.__playlist_create(ctx, name)
        if not playlist:
            return None

        player = self.get_player(ctx)

        queue = player.play_queue
        if is_history:
            queue = player.history

        self.logger.info(f'Saving queue contents to playlist "{name}", is_history? {is_history}')

        if queue.empty():
            return await ctx.send('There are no songs to add to playlist',
                                  delete_after=self.delete_after)
        for data in queue._queue: #pylint:disable=protected-access
            playlist_item = self.__playlist_add_item(ctx, playlist, data['id'], data['title'], data['uploader'])
            if playlist_item:
                await ctx.send(f'Added item "{data["title"]}" to playlist', delete_after=self.delete_after)
                continue
            await ctx.send(f'Unable to add playlist item "{data["title"]}", likely already exists', delete_after=self.delete_after)
        await ctx.send(f'Finished adding items to playlist "{name}"')
        if is_history:
            player.history.clear()
            await ctx.send('Cleared history')
        return

    @playlist.command(name='queue')
    async def playlist_queue(self, ctx, playlist_index, sub_command: typing.Optional[str] = ''): #pylint:disable=too-many-branches
        '''
        Add playlist to queue

        playlist_index: integer [Required]
            ID of playlist
        Sub commands - [shuffle]
            shuffle - Shuffle playlist when entering it into queue
        '''
        return await self.retry_command(self.__playlist_queue, ctx, playlist_index, sub_command)

    async def __playlist_queue(self, ctx, playlist_index, sub_command):
        if not await self.__check_database_session(ctx):
            return ctx.send('Database not set, cannot use playlist functions', delete_after=self.delete_after)

        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None
        shuffle = False
        # Make sure sub command is valid
        if sub_command:
            if sub_command.lower() == 'shuffle':
                shuffle = True
            else:
                return await ctx.send(f'Invalid sub command {sub_command}',
                                      delete_after=self.delete_after)

        vc = ctx.voice_client
        if not vc:
            await ctx.invoke(self.connect_)
        player = self.get_player(ctx)

        self.logger.info(f'Playlist queue called for playlist "{playlist_index}" in server "{ctx.guild.id}"')
        query = self.db_session.query(PlaylistItem).\
            filter(PlaylistItem.playlist_id == playlist.id)
        playlist_items = [item for item in query]

        if shuffle:
            await ctx.send('Shuffling playlist items',
                           delete_after=self.delete_after)
            random.shuffle(playlist_items)

        for item in playlist_items:
            await player.ytdl.check_source(ctx, f'{item.video_id}', self.bot.loop,
                                         max_song_length=self.max_song_length, download_queue=player.download_queue)
        await ctx.send(f'Added all songs in playlist "{playlist.name}" to queue',
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
        return await self.retry_command(self.__playlist_cleanup, ctx, playlist_index)

    async def __playlist_cleanup(self, ctx, playlist_index):
        if not await self.__check_database_session(ctx):
            return ctx.send('Database not set, cannot use playlist functions', delete_after=self.delete_after)

        vc = ctx.voice_client

        if not vc:
            await ctx.invoke(self.connect_)

        player = self.get_player(ctx)

        self.logger.info(f'Playlist cleanup called on index "{playlist_index}" in server "{ctx.guild.id}"')
        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None

        for item in self.db_session.query(PlaylistItem).filter(PlaylistItem.playlist_id == playlist.id):
            source_dicts = await player.ytdl.check_source(ctx, f'{item.video_id}', self.bot.loop)
            if source_dicts is None:
                self.logger.info(f'Unable to find source for "{item.title}", removing from database')
                await ctx.send(f'Unable to find youtube source ' \
                                f'for "{item.title}", "{item.video_id}", removing item from database',
                                delete_after=self.delete_after)
                self.db_session.delete(item)
                self.db_session.commit()
        self.logger.info(f'Finished cleanup for all items in playlist "{playlist.id}"')
        await ctx.send(f'Checked all songs in playlist "{playlist.name}"',
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
        return await self.retry_command(self.__playlist_merge, ctx, playlist_index_one, playlist_index_two)

    async def __playlist_merge(self, ctx, playlist_index_one, playlist_index_two):
        if not await self.__check_database_session(ctx):
            return ctx.send('Database not set, cannot use playlist functions', delete_after=self.delete_after)

        self.logger.info(f'Calling playlist merge of "{playlist_index_one}" and "{playlist_index_two}" in server "{ctx.guild.id}"')
        playlist_one = await self.__get_playlist(playlist_index_one, ctx)
        playlist_two = await self.__get_playlist(playlist_index_two, ctx)
        if not playlist_one:
            return ctx.send(f'Cannot find playlist {playlist_index_one}', delete_after=self.delete_after)
        if not playlist_two:
            return ctx.send(f'Cannot find playlist {playlist_index_two}', delete_after=self.delete_after)
        query = self.db_session.query(PlaylistItem).\
            filter(PlaylistItem.playlist_id == playlist_two.id)
        for item in query:
            playlist_item = self.__playlist_add_item(ctx, playlist_one, item.video_id, item.title, item.uploader)
            if playlist_item:
                await ctx.send(f'Added item "{item.title}" to playlist {playlist_index_one}', delete_after=self.delete_after)
                continue
            await ctx.send(f'Unable to add playlist item "{item.title}", likely already exists', delete_after=self.delete_after)
        await self.__playlist_delete(ctx, playlist_index_two)
