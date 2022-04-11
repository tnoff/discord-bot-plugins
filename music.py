import asyncio
from copy import deepcopy
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
import random
import tempfile
import typing

from async_timeout import timeout
from discord import HTTPException, FFmpegPCMAudio
from discord.ext import commands
from moviepy.editor import AudioFileClip, afx
from sqlalchemy import Column, Integer, String
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
QUEUE_MAX_SIZE_DEFAULT = 35

# Max song length
MAX_SONG_LENGTH_DEFAULT = 60 * 15


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
            table = f'{table} {item["title"]:48} || {uploader:16}'
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
            uploader = clean_string(item['uploader'], max_length=16)
        items.append({
            'title': clean_string(item['title'], max_length=48),
            'uploader': uploader,
            'duration': item['duration'],
        })

    table_strings = get_table_view(items, show_queue_time=True)
    header = f'```{"Pos":3} || {"Queue Time":10} || {"Title":48} || {"Uploader":16}```'
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

class YTDLClient():
    '''
    Youtube DL Source
    '''
    def __init__(self, ytdl_options, download_dir, logger):
        self.ytdl_options = ytdl_options
        self.logger = logger
        self.download_dir = download_dir

    def __getitem__(self, item: str):
        '''
        Allows us to access attributes similar to a dict.

        This is only useful when you are NOT downloading.
        '''
        return self.__getattribute__(item)

    async def create_source(self, ctx, search, loop, max_song_length=None, download=True):
        '''
        Create source from youtube search
        '''
        loop = loop or asyncio.get_event_loop()
        self.logger.info(f'{ctx.author} playing song with search {search}')
        to_run = partial(self.prepare_data_source, search=search, guild_id=ctx.guild.id,
                         max_song_length=max_song_length, requester=ctx.author,
                         download=download)
        return await loop.run_in_executor(None, to_run)

    def prepare_data_source(self, search, guild_id, max_song_length, requester, download):
        '''
        Prepare source from youtube url
        '''
        options = deepcopy(self.ytdl_options)
        guild_path = self.download_dir / f'{guild_id}'
        guild_path.mkdir(exist_ok=True, parents=True)
        # Add datetime to output here to account for playing same song multiple times
        options['outtmpl']  = str(guild_path / f'{int(datetime.now().timestamp())}.%(extractor)s-%(id)s-%(title)s.%(ext)s')
        ytdl = YoutubeDL(options)
        # Check song length before we download
        try:
            data_entries = ytdl.extract_info(url=search, download=False)
        except DownloadError:
            self.logger.error(f'Error downloading youtube search {search}')
            return None

        # We dont want to grab a full playlist of results, unless a specific playlist is passed
        # Easiest way to check this is if the search passed in used youtube urls directly
        direct_search = 'https://www.youtube.com' in search or 'https://youtu.be' in search
        if 'entries' in data_entries:
            data_entries = data_entries['entries']
            if not direct_search:
                data_entries = [data_entries[0]]

        # Do a quick check if its a dict type, sometimes a direct search will not be a list
        if isinstance(data_entries, dict):
            data_entries = [data_entries]

        if not download:
            return data_entries

        return_data = []
        for data in data_entries:
            # If song too long, skip
            if data['duration'] > max_song_length:
                return_data.append(data)
                continue
            try:
                data = ytdl.extract_info(url=data['webpage_url'], download=True)
            except DownloadError:
                self.logger.error(f'Error downloading youtube search {search}')
                return_data.append(None)
            self.logger.info(f'Starting download of video "{data["title"]}" from url "{data["webpage_url"]}"')
            file_path = Path(ytdl.prepare_filename(data))
            # The modified time of download videos can be the time when it was actually uploaded to youtube
            # Touch here to update the modified time, so that the cleanup check works as intendend
            file_path.touch(exist_ok=True)
            self.logger.info(f'Downloaded url "{data["webpage_url"]} to file "{file_path}"')
            data['requester'] = requester
            data['file_path'] = file_path
            return_data.append(data)
        return return_data


class MusicPlayer:
    '''
    A class which is assigned to each guild using the bot for Music.

    This class implements a queue and loop, which allows for different guilds
    to listen to different playlists simultaneously.

    When the bot disconnects from the Voice it's instance will be destroyed.
    '''

    def __init__(self, ctx, logger, ytdl_options, max_song_length, queue_max_size):
        self.bot = ctx.bot
        self.logger = logger
        self._guild = ctx.guild
        self._channel = ctx.channel
        self._cog = ctx.cog
        self.ytdl_options = ytdl_options
        self.history_size = 10

        self.logger.info(f'Max length for music queue in guild {self._guild} is {queue_max_size}')
        self.queue = MyQueue(maxsize=queue_max_size)
        self.history = MyQueue(maxsize=self.history_size)
        self.next = asyncio.Event()

        self.np = None  # Now playing message
        self.queue_messages = [] # Show current queue
        self.np_string = None # Keep np message here in case we pause
        self.queue_strings = None # Keep string here in case we pause
        self.volume = 1
        self.max_song_length = max_song_length
        self.current_path = None

        ctx.bot.loop.create_task(self.player_loop())

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
                    source_dict = await self.queue.get()
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
            message = f'Now playing {source_dict["webpage_url"]} requested by {source_dict["requester"].name}'
            self.np_string = message
            self.np = await self._channel.send(message)

            self.queue_messages = []
            self.queue_strings = get_queue_message(self.queue)
            if self.queue_strings is not None:
                for table in self.queue_strings:
                    self.queue_messages.append(await self._channel.send(table))

            await self.next.wait()

            # Make sure the FFmpeg process is cleaned up.
            source.cleanup()
            if not get_editing_path(source_dict['file_path']).exists():
                source_dict['file_path'].unlink()
            self.current_path = None

            try:
                self.history.put_nowait(source_dict)
            except asyncio.QueueFull:
                await self.history.get()
                self.history.put_nowait(source_dict)

            try:
                # We are no longer playing this song...
                await self.np.delete()
                for queue_message in self.queue_messages:
                    await queue_message.delete()
            except HTTPException:
                pass

    async def clear_remaining_queue(self):
        '''
        Delete files downloaded for queue
        '''
        while True:
            try:
               # 5 seconds here is kind of random time, but wait if any leftover downloads happening
                async with timeout(2):
                    source_dict = await self.queue.get()
                    remove_file_path(source_dict['file_path'])
            except asyncio.TimeoutError:
                break

    async def destroy(self, guild):
        '''
        Disconnect and cleanup the player.
        '''
        self.logger.info(f'Removing music bot from guild {self._guild}')
        await self.clear_remaining_queue()
        self.bot.loop.create_task(self._cog.cleanup(guild))


class Music(CogHelper): #pylint:disable=too-many-public-methods
    '''
    Music related commands
    '''

    def __init__(self, bot, db_engine, logger, settings):
        super().__init__(bot, db_engine, logger, settings)
        BASE.metadata.create_all(self.db_engine)
        BASE.metadata.bind = self.db_engine
        self.players = {}
        self.delete_after = settings.get('music_message_delete_after', DELETE_AFTER_DEFAULT)
        self.queue_max_size = settings.get('music_queue_max_size', QUEUE_MAX_SIZE_DEFAULT)
        self.max_song_length = settings.get('music_max_song_length', MAX_SONG_LENGTH_DEFAULT)
        self.download_dir = settings.get('music_download_dir', None)

        if self.download_dir is not None:
            self.download_dir = Path(self.download_dir)
            if not self.download_dir.exists():
                self.download_dir.mkdir(exist_ok=True, parents=True)
        else:
            self.download_dir = Path(tempfile.TemporaryDirectory().name) #pylint:disable=consider-using-with

        ytdlopts = {
            'format': 'bestaudio',
            'restrictfilenames': True,
            'noplaylist': True,
            'nocheckcertificate': True,
            'ignoreerrors': False,
            'logtostderr': False,
            'logger': logger,
            'default_search': 'auto',
            'source_address': '0.0.0.0'  # ipv6 addresses cause issues sometimes
        }
        self.ytdl = YTDLClient(ytdlopts, self.download_dir, logger)
        self.ytdl_options = ytdlopts
        self.bot.loop.create_task(self.modify_files(self.bot.loop))

    def __edit_audio_file(self, file_path):
        finished_path = get_finished_path(file_path)
        if finished_path.exists():
            self.logger.warning(f'Finished path "{str(finished_path)}" already exists, skipping')
            return finished_path
        editing_path = get_editing_path(file_path)
        try:
            edited_audio = AudioFileClip(str(file_path)).fx(afx.audio_normalize) #pylint:disable=no-member
            edited_audio.write_audiofile(str(editing_path))
            editing_path.rename(finished_path)
            return finished_path
        except OSError:
            # File likely was deleted in middle
            return None

    async def modify_files(self, loop):
        '''
        Go through a loop of all files, in order from old to new

        If file older than a day, assume it can be deleted
        Otherwise normalize audio on file
        '''
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():
            loop = loop or asyncio.get_event_loop()
            self.logger.debug('Attempting to edit music files')
            for file_path in self.download_dir.glob('**/*'):
                if file_path.is_dir():
                    continue
                self.logger.debug(f'Found music file "{file_path}"')
                # Check if guild player is still in use
                # Guild should be last dir before file
                guild_id = file_path.parent.name
                try:
                    _player = self.players[int(guild_id)]
                except KeyError:
                    self.logger.debug(f'Player for guild "{guild_id}" no longer exists, removing file')
                    file_path.unlink()
                    continue
                if file_path.suffix == '.part':
                    self.logger.warning(f'Ignoring file "{str(file_path)}" since not fully downloaded')
                    continue
                if (file_path.parent / (file_path.name + '.part')).exists():
                    self.logger.warning(f'Ignoring file "{str(file_path)}" since not fully downloaded')
                    continue
                # Check if file being used by any player
                should_skip = False
                for guild_id, player in self.players.items():
                    if player.current_path is None:
                        continue
                    if str(player.current_path.resolve()) == str(file_path.resolve()):
                        should_skip = True
                        self.logger.warning(f'Ignoring file "{str(file_path)}", being used in player in guild "{guild_id}"')
                        break
                if should_skip:
                    continue
                # Check if file is finished or being edited
                if ('.finished' in file_path.suffixes or '.edited' in file_path.suffixes) and '.mp3' in file_path.suffixes:
                    self.logger.warning(f'Ignoring file "{str(file_path)}"')
                    continue
                self.logger.info(f'Editing music file "{str(file_path)}"')
                # Use a timeout here, has shown to be a bit picky
                to_run = partial(self.__edit_audio_file, file_path=file_path)
                await loop.run_in_executor(None, to_run)
                # Sleep so other tasks can run, these runs seem to take a while
                await asyncio.sleep(.01)
            self.logger.debug('Done editing music files')
            await asyncio.sleep(180)

    async def cleanup(self, guild):
        '''
        Cleanup guild player
        '''
        try:
            await guild.voice_client.disconnect()
        except AttributeError:
            pass

        try:
            await self.players[guild.id].clear_remaining_queue()
            del self.players[guild.id]
        except KeyError:
            pass

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
            player = MusicPlayer(ctx, self.logger, self.ytdl_options, self.max_song_length, queue_max_size=self.queue_max_size)
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

        if player.queue.full():
            return await ctx.send('Queue is full, cannot add more songs',
                                  delete_after=self.delete_after)

        source_dicts = await self.ytdl.create_source(ctx, search, loop=self.bot.loop, max_song_length=self.max_song_length)
        for source_dict in source_dicts:
            if source_dict is None:
                await ctx.send(f'Unable to find youtube source for "{search}"',
                               delete_after=self.delete_after)
                continue
            if source_dict['duration'] > self.max_song_length:
                await ctx.send(f'Unable to add "<{source_dict["webpage_url"]}>"'
                               f' to queue, exceeded max length '
                               f'{self.max_song_length} seconds')
                continue

            try:
                player.queue.put_nowait(source_dict)
                self.logger.info(f'Adding "{source_dict["title"]}" '
                                f'to queue in guild {ctx.guild.id}')
                await ctx.send(f'Added "{source_dict["title"]}" to queue. '
                            f'"<{source_dict["webpage_url"]}>"',
                            delete_after=self.delete_after)
            except asyncio.QueueFull:
                await ctx.send('Queue is full, cannot add more songs',
                            delete_after=self.delete_after)

        # Reset queue messages
        for queue_message in player.queue_messages:
            try:
                await queue_message.delete()
            except HTTPException:
                pass

        player.queue_strings = get_queue_message(player.queue)
        player.queue_messages = []
        if player.queue_strings is not None:
            for table in player.queue_strings:
                player.queue_messages.append(await ctx.send(table))

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
        if player.queue.empty():
            return await ctx.send('There are currently no more queued songs.',
                                  delete_after=self.delete_after)
        player.queue.clear()
        await ctx.send('Cleared all items from queue',
                       delete_after=self.delete_after)

        # Reset queue messages
        for queue_message in player.queue_messages:
            await queue_message.delete()

    @commands.command(name='queue')
    async def queue_(self, ctx):
        '''
        Show current song queue
        '''
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await ctx.send('I am not currently playing anything',
                                  delete_after=self.delete_after)

        player = self.get_player(ctx)
        if player.queue.empty():
            return await ctx.send('There are currently no more queued songs.',
                                  delete_after=self.delete_after)

        # Delete any old queue message regardless of on/off
        for queue_message in player.queue_messages:
            await queue_message.delete()
        player.queue_messages = []

        player.queue_strings = get_queue_message(player.queue)
        if player.queue_strings is not None:
            for table in player.queue_strings:
                await ctx.send(f'{table}', delete_after=self.delete_after)

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

        history_strings = get_queue_message(player.history)
        if history_strings is not None:
            for table in history_strings:
                await ctx.send(f'{table}', delete_after=self.delete_after)

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
        if player.queue.empty():
            return await ctx.send('There are currently no more queued songs.',
                                  delete_after=self.delete_after)
        player.queue.shuffle()

        # Reset queue messages
        for queue_message in player.queue_messages:
            await queue_message.delete()

        player.queue_strings = get_queue_message(player.queue)
        player.queue_messages = []
        if player.queue_strings is not None:
            for table in player.queue_strings:
                await ctx.send(f'{table}', delete_after=self.delete_after)

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
        if player.queue.empty():
            return await ctx.send('There are currently no more queued songs.',
                                  delete_after=self.delete_after)

        try:
            queue_index = int(queue_index)
        except ValueError:
            return await ctx.send(f'Invalid queue index {queue_index}',
                                  delete_after=self.delete_after)

        item = player.queue.remove_item(queue_index)
        if item is None:
            return ctx.send(f'Unable to remove queue index {queue_index}',
                            delete_after=self.delete_after)
        await ctx.send(f'Removed item {item["title"]} from queue',
                       delete_after=self.delete_after)
        remove_file_path(item['file_path'])
        # Reset queue messages
        for queue_message in player.queue_messages:
            try:
                await queue_message.delete()
            except HTTPException:
                pass

        player.queue_strings = get_queue_message(player.queue)
        player.queue_messages = []
        if player.queue_strings is not None:
            for table in player.queue_strings:
                player.queue_messages.append(await ctx.send(table))

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
        if player.queue.empty():
            return await ctx.send('There are currently no more queued songs.',
                                  delete_after=self.delete_after)

        try:
            queue_index = int(queue_index)
        except ValueError:
            return await ctx.send(f'Invalid queue index {queue_index}',
                                  delete_after=self.delete_after)

        item = player.queue.bump_item(queue_index)
        if item is None:
            return await ctx.send(f'Unable to bump queue index {queue_index}',
                            delete_after=self.delete_after)
        await ctx.send(f'Bumped item {item["title"]} to top of queue',
                       delete_after=self.delete_after)

        # Reset queue messages
        for queue_message in player.queue_messages:
            try:
                await queue_message.delete()
            except HTTPException:
                pass

        player.queue_strings = get_queue_message(player.queue)
        player.queue_messages = []
        if player.queue_strings is not None:
            for table in player.queue_strings:
                player.queue_messages.append(await ctx.send(table))

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

    async def __get_playlist(self, playlist_index, ctx): #pylint:disable=no-self-use
        try:
            index = int(playlist_index)
        except ValueError:
            await ctx.send(f'Invalid playlist index {playlist_index}', delete_after=self.delete_after)
            return None
        playlist_items = self.db_session.query(Playlist).\
            filter(Playlist.server_id == str(ctx.guild.id))
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
            ctx.send('Database not set, cannot use playlist functions', delete=self.delete_after)
            return None
        playlist = Playlist(name=clean_string(name, max_length=256), server_id=ctx.guild.id)
        try:
            self.db_session.add(playlist)
            self.db_session.commit()
        except IntegrityError:
            self.db_session.rollback()
            await ctx.send(f'Unable to create playlist "{name}", name likely already exists')
            return None
        self.logger.info(f'Playlist created {playlist.id} in guild {ctx.guild.id}')
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
        if not await self.__check_database_session(ctx):
            return ctx.send('Database not set, cannot use playlist functions', delete=self.delete_after)
        playlist_items = self.db_session.query(Playlist).\
            filter(Playlist.server_id == str(ctx.guild.id))
        playlist_items = [p for p in playlist_items]

        if not playlist_items:
            return await ctx.send('No playlists in database',
                                  delete_after=self.delete_after)
        table = ''
        for (count, playlist) in enumerate(playlist_items):
            table = f'{table}{count +1:3} || {clean_string(playlist.name):64}\n'
        return await ctx.send(f'```{table}```', delete_after=self.delete_after)

    def __playlist_add_item(self, ctx, playlist, data):
        self.logger.info(f'Adding video_id {data["id"]} to playlist {playlist.id} '
                         f' in guild {ctx.guild.id}')
        playlist_item = PlaylistItem(title=clean_string(data['title'], max_length=256),
                                     video_id=data['id'],
                                     uploader=clean_string(data['uploader'], max_length=256),
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
        if not await self.__check_database_session(ctx):
            return ctx.send('Database not set, cannot use playlist functions', delete=self.delete_after)

        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None

        source_dicts = await self.ytdl.create_source(ctx, search, loop=self.bot.loop, download=False)
        for data in source_dicts:
            if data is None:
                await ctx.send(f'Unable to find video for search {search}')
            self.logger.info(f'Adding video_id {data["id"]} to playlist {playlist.id} '
                            f' in guild {ctx.guild.id}')
            playlist_item = self.__playlist_add_item(ctx, playlist, data)
            if playlist_item:
                await ctx.send(f'Added item {data["title"]} to playlist {playlist_index}', delete_after=self.delete_after)
            await ctx.send('Unable to add playlist item, likely already exists', delete_after=self.delete_after)

    @playlist.command(name='item-remove')
    async def playlist_item_remove(self, ctx, playlist_index, song_index):
        '''
        Add item to playlist

        playlist_index: integer [Required]
            ID of playlist
        song_index: integer [Required]
            ID of song to remove
        '''
        if not await self.__check_database_session(ctx):
            return ctx.send('Database not set, cannot use playlist functions', delete=self.delete_after)

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
        if not await self.__check_database_session(ctx):
            return ctx.send('Database not set, cannot use playlist functions', delete=self.delete_after)

        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None

        query = self.db_session.query(PlaylistItem).\
            filter(PlaylistItem.playlist_id == playlist.id)
        items = []
        for item in query:
            uploader = ''
            if item.uploader is not None:
                uploader = clean_string(item.uploader)
            items.append({
                'title': clean_string(item.title),
                'uploader': uploader,
            })
        if not items:
            return await ctx.send('No playlist items in database',
                                  delete_after=self.delete_after)

        tables = get_table_view(items)
        header = f'```{"Pos":3} || {"Title":48} || {"Uploader":16}```'
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
        if not await self.__check_database_session(ctx):
            return ctx.send('Database not set, cannot use playlist functions', delete=self.delete_after)

        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None
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
        if not await self.__check_database_session(ctx):
            return ctx.send('Database not set, cannot use playlist functions', delete=self.delete_after)

        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None
        playlist.name = playlist_name
        self.db_session.commit()
        return await ctx.send(f'Renamed playlist {playlist_index} to name {playlist_name}')

    @playlist.command(name='queue-save')
    async def playlist_queue_save(self, ctx, *, name: str):
        '''
        Save contents of queue to a new playlist

        name: str [Required]
            Name of new playlist to create
        '''
        playlist = await self.__playlist_create(ctx, name)
        if not playlist:
            return None

        player = self.get_player(ctx)
        if player.queue.empty():
            return await ctx.send('There are currently no more queued songs.',
                                  delete_after=self.delete_after)

        for data in player.queue:
            playlist_item = self.__playlist_add_item(ctx, playlist, data)
            if playlist_item:
                await ctx.send(f'Added item {data["title"]} to playlist', delete_after=self.delete_after)
                continue
            await ctx.send(f'Unable to add playlist item {data["title"]}, likely already exists', delete_after=self.delete_after)
        return await ctx.send(f'Finished adding queue items to playlist {name}')

    @playlist.command(name='queue')
    async def playlist_queue(self, ctx, playlist_index, sub_command: typing.Optional[str] = ''): #pylint:disable=too-many-branches
        '''
        Add playlist to queue

        playlist_index: integer [Required]
            ID of playlist
        Sub commands - [shuffle]
            shuffle - Shuffle playlist when entering it into queue
        '''
        if not await self.__check_database_session(ctx):
            return ctx.send('Database not set, cannot use playlist functions', delete=self.delete_after)

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

        query = self.db_session.query(PlaylistItem).\
            filter(PlaylistItem.playlist_id == playlist.id)
        playlist_items = [item for item in query]

        if shuffle:
            await ctx.send('Shuffling playlist items',
                           delete_after=self.delete_after)
            random.shuffle(playlist_items)

        for item in playlist_items:
            if player.queue.full():
                return await ctx.send('Queue is full, cannot add more songs',
                                      delete_after=self.delete_after)

            source_dicts = await self.ytdl.create_source(ctx, f'{item.video_id}',
                                                         loop=self.bot.loop,
                                                         max_song_length=self.max_song_length)
            for source_dict in source_dicts:
                if source_dict is None:
                    await ctx.send(f'Unable to find youtube source ' \
                                    f'for "{item.title}", "{item.video_id}"',
                                    delete_after=self.delete_after)
                # For backwards compat, if uploader not set, add it here
                if item.uploader is None:
                    item.uploader = clean_string(source_dict['uploader'], max_length=256)
                    self.db_session.commit()

                    continue
                if source_dict['duration'] > self.max_song_length:
                    await ctx.send(f'Unable to add <{source_dict["webpage_url"]}>'
                                    f' to queue, exceeded max length '
                                    f'{self.max_song_length} seconds', delete_after=self.delete_after)
                    continue
                try:
                    player.queue.put_nowait(source_dict)
                    await ctx.send(f'Added "{source_dict["title"]}" to queue. '
                                f'<{source_dict["webpage_url"]}>',
                                delete_after=self.delete_after)
                except asyncio.QueueFull:
                    await ctx.send('Queue is full, cannot add more songs',
                                delete_after=self.delete_after)
                    break

        await ctx.send(f'Added all songs in playlist {playlist.name} to Queue',
                       delete_after=self.delete_after)

        # Reset queue messages
        for queue_message in player.queue_messages:
            try:
                await queue_message.delete()
            except HTTPException:
                pass

        player.queue_strings = get_queue_message(player.queue)
        player.queue_messages = []
        if player.queue_strings is not None:
            for table in player.queue_strings:
                player.queue_messages.append(await ctx.send(table))
