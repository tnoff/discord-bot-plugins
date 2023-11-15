from asyncio import sleep
from datetime import datetime, timedelta
from random import choice
from pathlib import Path
from re import match, sub, MULTILINE
from tempfile import NamedTemporaryFile
from typing import Optional

from discord import TextChannel
from discord.ext import commands
from discord.errors import NotFound, HTTPException, DiscordServerError
from jsonschema import ValidationError
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy import ForeignKey, UniqueConstraint

from discord_bot.cogs.common import CogHelper
from discord_bot.database import BASE
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.utils import retry_command, async_retry_command, validate_config

# Make length of a leader or follower word
MAX_WORD_LENGTH = 255

# Default for how many days to keep messages around
MARKOV_HISTORY_RETENTION_DAYS_DEFAULT = 365

# Default for how to wait between each loop
LOOP_SLEEP_INTERVAL_DEFAULT = 300

# Limit for how many messages we grab on each history check
MESSAGE_CHECK_LIMIT = 16

# Markov config schema
MARKOV_SECTION_SCHEMA = {
    'type': 'object',
    'properties': {
        'loop_sleep_interval': {
            'type': 'number',
        },
        'message_check_limit': {
            'type': 'number',
        },
        'history_retention_days': {
            'type': 'number',

        },
    }
}

def retry_discord_message_command(func, *args, **kwargs):
    '''
    Retry discord send message command, catch case of rate limiting
    '''
    exceptions = (HTTPException, DiscordServerError)
    return retry_command(func, *args, **kwargs, accepted_exceptions=exceptions)

async def async_retry_discord_message_command(func, *args, **kwargs):
    '''
    Retry discord send message command, catch case of rate limiting
    '''
    exceptions = (HTTPException, DiscordServerError)
    return await async_retry_command(func, *args, **kwargs, accepted_exceptions=exceptions)

def clean_message(content, emoji_ids):
    '''
    Clean channel message
    content :   Full message content to clean
    emojis  :   List of server emoji ids, so we can remove any not from server

    Returns "corpus", list of cleaned words
    '''
    # Remove web links and mentions from text
    message_text = sub(r'(https?\://|\<\@)\S+', '',
                       content, flags=MULTILINE)
    # Doesnt remove @here or @everyone
    message_text = message_text.replace('@here', '')
    message_text = message_text.replace('@everyone', '')
    # Strip blank ends
    message_text = message_text.strip()
    corpus = []
    for word in message_text.split(' '):
        if word in ('', ' '):
            continue
        # Check for commands again
        if word[0] == '!':
            continue
        # Check for emojis in message
        # If emoji, check if belongs to list, if not, disregard it
        # Emojis can be case sensitive so do not lower them
        # Custom emojis usually have <:emoji:id> format
        # Ex: <:fail:1231031923091032910390>
        match_result = match('^\ *<(?P<emoji>:\w+:)(?P<id>\d+)>\ *$', word)
        if match_result:
            if int(match_result.group('id')) in emoji_ids:
                corpus.append(word)
            continue
        corpus.append(word.lower())
    return corpus

#
# Markov Tables
#

class MarkovChannel(BASE):
    '''
    Markov channel
    '''
    __tablename__ = 'markov_channel'
    __table_args__ = (
        UniqueConstraint('channel_id', 'server_id',
                         name='_unique_markov_channel'),
    )
    id = Column(Integer, primary_key=True)
    channel_id = Column(String(128))
    server_id = Column(String(128))
    last_message_id = Column(String(128))

class MarkovRelation(BASE):
    '''
    Markov Relation
    '''
    __tablename__ = 'markov_relation'
    id = Column(Integer, primary_key=True)
    channel_id = Column(Integer, ForeignKey('markov_channel.id'))
    leader_word = Column(String(MAX_WORD_LENGTH))
    follower_word = Column(String(MAX_WORD_LENGTH))
    created_at = Column(DateTime)


class ExitEarlyException(Exception):
    '''
    Exit early from tasks
    '''


class Markov(CogHelper):
    '''
    Save markov relations to a database periodically
    '''
    def __init__(self, bot, db_engine, logger, settings):
        super().__init__(bot, db_engine, logger, settings)
        BASE.metadata.create_all(self.db_engine)
        BASE.metadata.bind = self.db_engine
        try:
            validate_config(settings['markov'], MARKOV_SECTION_SCHEMA)
        except ValidationError as exc:
            raise CogMissingRequiredArg('Invalid config given for markov bot') from exc
        except KeyError:
            settings['markov'] = {}
        self.loop_sleep_interval = settings['markov'].get('loop_sleep_interval', LOOP_SLEEP_INTERVAL_DEFAULT)
        self.message_check_limit = settings['markov'].get('message_check_limit', MESSAGE_CHECK_LIMIT)
        self.history_retention_days = settings['markov'].get('history_retention_days', MARKOV_HISTORY_RETENTION_DAYS_DEFAULT)
        self.server_reject_list = settings['markov'].get('server_reject_list', [])

        self.lock_file = Path(NamedTemporaryFile(delete=False).name) #pylint:disable=consider-using-with
        self._task = None

    async def cog_load(self):
        self._task = self.bot.loop.create_task(self.main_loop())

    async def cog_unload(self):
        if self._task:
            self._task.cancel()
        if self.lock_file.exists():
            self.lock_file.unlink()

    def __ensure_word(self, word):
        if len(word) >= MAX_WORD_LENGTH:
            self.logger.warning(f'Markov :: Cannot add word "{word}", is too long')
            return None
        return word

    # https://srome.github.io/Making-A-Markov-Chain-Twitter-Bot-In-Python/
    def __build_and_save_relations(self, corpus, markov_channel, message_timestamp):
        for (k, word) in enumerate(corpus):
            if k != len(corpus) - 1: # Deal with last word
                next_word = corpus[k+1]
            else:
                next_word = corpus[0] # To loop back to the beginning
            leader_word = self.__ensure_word(word)
            if leader_word is None:
                continue
            follower_word = self.__ensure_word(next_word)
            if follower_word is None:
                continue
            new_relation = MarkovRelation(channel_id=markov_channel.id,
                                          leader_word=leader_word,
                                          follower_word=follower_word,
                                          created_at=message_timestamp)
            self.db_session.add(new_relation)
            self.db_session.commit()

    def _delete_channel_relations(self, channel_id):
        self.db_session.query(MarkovRelation).filter(MarkovRelation.channel_id == channel_id).delete()

    async def main_loop(self):
        '''
        Our main loop.
        '''
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():
            try:
                await self.__main_loop()
            except ExitEarlyException:
                return
            except Exception as e:
                self.logger.exception(e)
                print(f'Player loop exception {str(e)}')
                return

    async def __main_loop(self):
        '''
        Main loop runner
        '''
        retention_cutoff = datetime.utcnow() - timedelta(days= self.history_retention_days)
        self.logger.debug(f'Markov :: Entering message gather loop, using cutoff {retention_cutoff}')

        for markov_channel in self.db_session.query(MarkovChannel).all():
            await sleep(.01) # Sleep one second just in case someone called a command
            self.logger.debug(f'Markov :: Checking channel id: {markov_channel.channel_id}, server id: {markov_channel.server_id}')
            channel = await async_retry_discord_message_command(self.bot.fetch_channel, markov_channel.channel_id)
            server = await async_retry_discord_message_command(self.bot.fetch_guild, markov_channel.server_id)
            emoji_ids = [emoji.id for emoji in await server.fetch_emojis()]
            self.logger.info('Markov :: Gathering markov messages for '
                             f'channel {markov_channel.channel_id}')
            # Start at the beginning of channel history,
            # slowly make your way make to current day
            if not markov_channel.last_message_id:
                messages = [m async for m in retry_discord_message_command(channel.history, limit=self.message_check_limit, after=retention_cutoff, oldest_first=True)]
            else:
                try:
                    last_message = await async_retry_discord_message_command(channel.fetch_message, markov_channel.last_message_id)
                    messages = [m async for m in retry_discord_message_command(channel.history, after=last_message, limit=self.message_check_limit, oldest_first=True)]
                except NotFound:
                    self.logger.warning(f'Markov :: Unable to find message {markov_channel.last_message_id}'
                                        f' in channel {markov_channel.id}')
                    # Last message on record not found
                    # If this happens, wipe the channel clean and restart
                    self._delete_channel_relations(markov_channel.id)
                    markov_channel.last_message_id = None
                    self.db_session.commit()
                    # Skip this channel for now
                    continue

            if len(messages) == 0:
                self.logger.debug(f'Markov :: No new messages for channel {markov_channel.channel_id}')
                continue


            for message in messages:
                self.logger.debug(f'Markov :: Gathering message {message.id} '
                                    f'for channel {markov_channel.channel_id}')
                add_message = True
                if not message.content or message.author.bot:
                    add_message = False
                elif message.content[0] == '!':
                    add_message = False
                corpus = None
                if add_message:
                    corpus = clean_message(message.content, emoji_ids)
                if corpus:
                    self.logger.info(f'Attempting to add corpus "{corpus}" '
                                    f'to channel {markov_channel.channel_id}')
                    self.__build_and_save_relations(corpus, markov_channel, message.created_at)
                markov_channel.last_message_id = str(message.id)
                self.db_session.commit()
            self.logger.debug(f'Markov :: Done with channel {markov_channel.channel_id}')

        # Clean up old messages
        self.db_session.query(MarkovRelation).filter(MarkovRelation.created_at < retention_cutoff).delete()
        self.db_session.commit()
        self.logger.debug('Markov :: Deleted expired/old markov relations')
        await sleep(self.loop_sleep_interval) # Every 5 minutes

    @commands.group(name='markov', invoke_without_command=False)
    async def markov(self, ctx):
        '''
        Markov functions
        '''
        if ctx.invoked_subcommand is None:
            await ctx.send('Invalid sub command passed...')

    @markov.command(name='on')
    async def on(self, ctx):
        '''
        Turn markov on for channel
        '''
        if not await self.check_user_role(ctx):
            return await ctx.send('Unable to verify user role, ignoring command')

        if ctx.guild.id in self.server_reject_list:
            return await ctx.send('Unable to turn on markov for server, in reject list')

        # Ensure channel not already on
        markov = self.db_session.query(MarkovChannel).\
            filter(MarkovChannel.channel_id == str(ctx.channel.id)).\
            filter(MarkovChannel.server_id == str(ctx.guild.id)).first()

        if markov:
            return await ctx.send('Channel already has markov turned on')

        channel = await self.bot.fetch_channel(ctx.channel.id)
        if not isinstance(channel, TextChannel):
            await ctx.send('Channel is not text channel, cannot turn on markov')

        new_markov = MarkovChannel(channel_id=str(ctx.channel.id),
                                   server_id=str(ctx.guild.id),
                                   last_message_id=None)
        self.db_session.add(new_markov)
        self.db_session.commit()
        self.logger.info(f'Markov :: Adding new markov channel {ctx.channel.id} from server {ctx.guild.id}')
        return await ctx.send('Markov turned on for channel')

    @markov.command(name='off')
    async def off(self, ctx):
        '''
        Turn markov off for channel
        '''
        if not await self.check_user_role(ctx):
            return await ctx.send('Unable to verify user role, ignoring command')

        if ctx.guild.id in self.server_reject_list:
            return await ctx.send('Unable to turn off markov for server, in reject list')

        # Ensure channel not already on
        markov_channel = self.db_session.query(MarkovChannel).\
            filter(MarkovChannel.channel_id == str(ctx.channel.id)).\
            filter(MarkovChannel.server_id == str(ctx.guild.id)).first()

        if not markov_channel:
            return await ctx.send('Channel does not have markov turned on')
        self.logger.info(f'Markov :: Turning off markov channel {ctx.channel.id} from server {ctx.guild.id}')

        self._delete_channel_relations(markov_channel.id)
        self.db_session.delete(markov_channel)
        self.db_session.commit()
        return await ctx.send('Markov turned off for channel')

    @markov.command(name='speak')
    async def speak(self, ctx, #pylint:disable=too-many-locals
                    first_word: Optional[str] = '',
                    sentence_length: Optional[int] = 32):
        '''
        Say a random sentence generated by markov

        Note that this uses all markov channels setup for the server

        first_word  :   First word for markov string, if not given will be random.
        sentence_length :   Length of sentence

        Note that for first_word, multiple words can be given, but they must be in quotes
        Ex: !markov speak "hey whats up", or !markov speak "hey whats up" 64
        '''
        if not await self.check_user_role(ctx):
            return await ctx.send('Unable to verify user role, ignoring command')

        if ctx.guild.id in self.server_reject_list:
            return await ctx.send('Unable to use markov for server, in reject list')

        self.logger.info(f'Markov :: Calling speak on server {ctx.guild.id}')
        all_words = []
        first = None
        if first_word:
            # Allow for multiple words to be given
            # If so, just grab last word
            starting_words = first_word.split(' ')
            # Make sure to add to all words here
            for start_words in starting_words[:-1]:
                all_words.append(start_words.lower())
            first = starting_words[-1].lower()


        query = self.db_session.query(MarkovRelation.id).\
                    join(MarkovChannel, MarkovChannel.id == MarkovRelation.channel_id).\
                    filter(MarkovChannel.server_id == str(ctx.guild.id))
        if first:
            query = query.filter(MarkovRelation.leader_word == first)

        possible_words = [word[0] for word in query]
        if len(possible_words) == 0:
            if first_word:
                return await ctx.send(f'No markov word matching "{first_word}"')
            return await ctx.send('No markov words to pick from')

        word = self.db_session.query(MarkovRelation).get(choice(possible_words)).leader_word
        all_words.append(word)

        # Save a cache layer to reduce db calls
        for _ in range(sentence_length + 1):
            # Get all leader ids first so you can pass it in
            relation_ids = self.db_session.query(MarkovRelation.id).\
                    join(MarkovChannel, MarkovChannel.id == MarkovRelation.channel_id).\
                    filter(MarkovChannel.server_id == str(ctx.guild.id)).\
                    filter(MarkovRelation.leader_word == word)
            relation_ids = [i[0] for i in relation_ids]
            # Get random choice of leader ids
            word = self.db_session.query(MarkovRelation).get(choice(relation_ids)).follower_word
            all_words.append(word)
        return await ctx.send(' '.join(markov_word for markov_word in all_words))
