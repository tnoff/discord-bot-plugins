from asyncio import sleep
from datetime import datetime, timedelta
from random import choice, choices
from re import match, sub, MULTILINE
from tempfile import NamedTemporaryFile
from typing import Optional

from lockfile import LockFile

from discord import TextChannel
from discord.ext import commands
from discord.errors import NotFound
from sqlalchemy import Boolean, Column, DateTime, Integer, String
from sqlalchemy import ForeignKey, UniqueConstraint

from discord_bot.cogs.common import CogHelper
from discord_bot.database import BASE


# Default for how many days to keep messages around
MARKOV_HISTORY_RETENTION_DAYS = 365

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
    is_private = Column(Boolean)

class MarkovWord(BASE):
    '''
    Markov word
    '''
    __tablename__ = 'markov_word'
    id = Column(Integer, primary_key=True)
    word = Column(String(128))
    channel_id = Column(Integer, ForeignKey('markov_channel.id'))

class MarkovRelation(BASE):
    '''
    Markov Relation
    '''
    __tablename__ = 'markov_relation'
    id = Column(Integer, primary_key=True)
    leader_id = Column(Integer, ForeignKey('markov_word.id'))
    follower_id = Column(Integer, ForeignKey('markov_word.id'))
    created_at = Column(DateTime)


class Markov(CogHelper):
    '''
    Save markov relations to a database periodically
    '''
    def __init__(self, bot, db_engine, logger, settings):
        super().__init__(bot, db_engine, logger, settings)
        BASE.metadata.create_all(self.db_engine)
        BASE.metadata.bind = self.db_engine
        if 'markov_history_rention_days' not in settings:
            self.settings['markov_history_rention_days'] = MARKOV_HISTORY_RETENTION_DAYS
        # Keep lock file for later
        self.lock_file = NamedTemporaryFile(delete=False) #pylint:disable=consider-using-with
        self.lock = LockFile(self.lock_file.name)

        self.bot.loop.create_task(self.wait_loop())

    def __ensure_word(self, word, markov_channel):
        markov_word = self.db_session.query(MarkovWord).\
                filter(MarkovWord.word == word).\
                filter(MarkovWord.channel_id == markov_channel.id).first()
        if markov_word:
            return markov_word
        if len(word) > 1024:
            self.logger.warning(f'Cannot add word "{word}", is too long')
            return None
        new_word = MarkovWord(word=word, channel_id=markov_channel.id)
        self.db_session.add(new_word)
        self.db_session.commit()
        self.db_session.flush()
        return new_word

    # https://srome.github.io/Making-A-Markov-Chain-Twitter-Bot-In-Python/
    def __build_and_save_relations(self, corpus, markov_channel, message_timestamp):
        for (k, word) in enumerate(corpus):
            if k != len(corpus) - 1: # Deal with last word
                next_word = corpus[k+1]
            else:
                next_word = corpus[0] # To loop back to the beginning

            leader_word = self.__ensure_word(word, markov_channel)
            if leader_word is None:
                continue
            follower_word = self.__ensure_word(next_word, markov_channel)
            if follower_word is None:
                continue
            new_relation = MarkovRelation(leader_id=leader_word.id,
                                          follower_id=follower_word.id,
                                          created_at=message_timestamp)
            self.db_session.add(new_relation)
            self.db_session.commit()

    def _delete_channel_words(self, channel_id):
        markov_words = self.db_session.query(MarkovWord.id).\
                        filter(MarkovWord.channel_id == channel_id)
        self.db_session.query(MarkovRelation).\
            filter(MarkovRelation.leader_id.in_(markov_words.subquery())).\
            delete(synchronize_session=False)
        self.db_session.query(MarkovWord).\
            filter(MarkovWord.channel_id == channel_id).delete()


    async def wait_loop(self):
        '''
        Our main loop.
        '''
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():

            self.logger.debug('Markov - Message loop waiting to acquire lock')
            self.lock.acquire()

            retention_cutoff = datetime.utcnow() - timedelta(days=self.settings['markov_history_rention_days'])
            self.logger.debug(f'Using cutoff {retention_cutoff} for markov bot')
            for markov_channel in self.db_session.query(MarkovChannel).all():
                channel = await self.bot.fetch_channel(markov_channel.channel_id)
                server = await self.bot.fetch_guild(markov_channel.server_id)
                emoji_ids = [emoji.id for emoji in await server.fetch_emojis()]
                self.logger.info('Gathering markov messages for '
                                 f'channel {markov_channel.channel_id}')
                # Start at the beginning of channel history,
                # slowly make your way make to current day
                if not markov_channel.last_message_id:
                    messages = await channel.history(limit=128, after=retention_cutoff, oldest_first=True).flatten()
                else:
                    try:
                        last_message = await channel.fetch_message(markov_channel.last_message_id)
                        messages = await channel.history(after=last_message, limit=128).flatten()
                    except NotFound:
                        self.logger.error(f'Unable to find message {markov_channel.last_message_id}'
                                          f' in channel {markov_channel.id}')
                        # Last message on record not found
                        # If this happens, wipe the channel clean and restart
                        self._delete_channel_words(markov_channel.id)
                        markov_channel.last_message_id = None
                        self.db_session.commit()
                        # Skip this channel for now
                        continue

                if len(messages) == 0:
                    self.logger.debug(f'No new messages for channel {markov_channel.channel_id}')
                    continue

                for message in messages:
                    self.logger.debug(f'Gathering message {message.id} '
                                      f'for channel {markov_channel.channel_id}')
                    markov_channel.last_message_id = message.id
                    # Skip messages older than retention date
                    if message.created_at < retention_cutoff:
                        self.logger.warning(f'Message {message.id} too old for channel, skipping')
                        continue
                    # If no content continue or from a bot skip
                    if not message.content or message.author.bot:
                        continue
                    # If message begins with '!', assume it was a bot command
                    if message.content[0] == '!':
                        continue
                    corpus = clean_message(message.content, emoji_ids)
                    if not corpus:
                        continue
                    self.logger.info(f'Attempting to add corpus "{corpus}" '
                                     f'to channel {markov_channel.channel_id}')
                    self.__build_and_save_relations(corpus, markov_channel, message.created_at)
                # Commit at the end in case the last message was skipped
                self.db_session.commit()

            # Clean up old messages
            self.db_session.query(MarkovRelation).filter(MarkovRelation.created_at < retention_cutoff).delete()
            # Wait until next loop
            self.lock.release()
            self.logger.debug('Waiting 30 minutes for next markov iteration')
            await sleep(1800) # Every 30 minutes

    @commands.group(name='markov', invoke_without_command=False)
    async def markov(self, ctx): #pylint:disable=no-self-use
        '''
        Markov functions
        '''
        if ctx.invoked_subcommand is None:
            await ctx.send('Invalid sub command passed...')

    @markov.command(name='on')
    async def on(self, ctx, channel_type: Optional[str] = 'public'):
        '''
        Turn markov on for channel
        channel_type    :   Either "private" or "public", will be "public" by default
        '''
        if channel_type.lower() not in ['public', 'private']:
            return await ctx.send(f'Invalid channel type "{channel_type}"')
        is_private = channel_type.lower() == 'private'

        # Ensure channel not already on
        markov = self.db_session.query(MarkovChannel).\
            filter(MarkovChannel.channel_id == str(ctx.channel.id)).\
            filter(MarkovChannel.server_id == str(ctx.guild.id)).first()

        if markov:
            if is_private != markov.is_private:
                markov.is_private = is_private
                self.db_session.commit()
                return await ctx.send(f'Updating channel type to {channel_type.lower()}')
            return await ctx.send('Channel already has markov turned on')

        channel = await self.bot.fetch_channel(ctx.channel.id)
        if not isinstance(channel, TextChannel):
            await ctx.send('Channel is not text channel, cannot turn on markov')

        new_markov = MarkovChannel(channel_id=str(ctx.channel.id),
                                   server_id=str(ctx.guild.id),
                                   last_message_id=None,
                                   is_private=is_private)
        self.db_session.add(new_markov)
        self.db_session.commit()
        self.logger.info(f'Adding new markov channel {ctx.channel.id} from server {ctx.guild.id}')

        return await ctx.send('Markov turned on for channel')

    @markov.command(name='off')
    async def off(self, ctx):
        '''
        Turn markov off for channel
        '''
        self.logger.debug('Markov - Off waiting for acquire lock')
        self.lock.acquire()
        # Ensure channel not already on
        markov_channel = self.db_session.query(MarkovChannel).\
            filter(MarkovChannel.channel_id == str(ctx.channel.id)).\
            filter(MarkovChannel.server_id == str(ctx.guild.id)).first()

        if not markov_channel:
            return await ctx.send('Channel does not have markov turned on')
        self.logger.info(f'Turning off markov channel {ctx.channel.id} from server {ctx.guild.id}')

        self._delete_channel_words(markov_channel.id)
        self.db_session.delete(markov_channel)
        self.db_session.commit()

        self.lock.release()
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
        self.logger.debug('Markov - Speak waiting to acquire lock')
        self.lock.acquire()
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

        # First check is channel is private
        markov_channel = self.db_session.query(MarkovChannel).\
            filter(MarkovChannel.channel_id == str(ctx.channel.id)).\
            filter(MarkovChannel.server_id == str(ctx.guild.id)).first()

        # Find first word, first get query of either all channels in server, or just
        # single channel

        # First get results from all public channels
        query = self.db_session.query(MarkovWord.id).\
                    join(MarkovChannel, MarkovChannel.id == MarkovWord.channel_id).\
                    filter(MarkovChannel.server_id == str(ctx.guild.id)).\
                    filter(MarkovChannel.is_private == False)
        if first:
            query = query.filter(MarkovWord.word == first)

        # If within a private channel, add this channels results to query
        if markov_channel and markov_channel.is_private:
            # Results either come from this private channel
            # or from another public channel within server
            second_query = self.db_session.query(MarkovWord.id).\
                            join(MarkovChannel, MarkovChannel.id == MarkovWord.channel_id).\
                            filter(MarkovWord.channel_id == markov_channel.id)
            if first:
                second_query = second_query.filter(MarkovWord.word == first)
            query = query.union(second_query)

        possible_word_ids = [word[0] for word in query]

        if len(possible_word_ids) == 0:
            if first_word:
                return await ctx.send(f'No markov word matching "{first_word}"')
            return await ctx.send('No markov words to pick from')
        word = self.db_session.query(MarkovWord).get(choice(possible_word_ids)).word
        all_words.append(word)

        # Save a cache layer to reduce db calls
        follower_cache = {}
        for _ in range(sentence_length + 1):
            try:
                _follower_choices = follower_cache[word]
            except KeyError:
                follower_cache[word] = {}

                # Get all leader ids first so you can pass it in
                # First get all leader ids from public channels
                leader_ids = self.db_session.query(MarkovWord.id).\
                        join(MarkovChannel, MarkovChannel.id == MarkovWord.channel_id).\
                        filter(MarkovChannel.server_id == str(ctx.guild.id)).\
                        filter(MarkovChannel.is_private == False).\
                        filter(MarkovWord.word == word)
                # If current channel is private, add this channels results to query
                if markov_channel and markov_channel.is_private:
                    # Results either come from this private channel
                    # or from another public channel within server
                    leader_ids = leader_ids.union(self.db_session.query(MarkovWord.id).\
                        filter(MarkovWord.channel_id == markov_channel.id).\
                        filter(MarkovChannel.server_id == str(ctx.guild.id)).\
                        filter(MarkovWord.word == word))

                # First generate basic ordered query to get the offsets from
                for _relation, follower_word in self.db_session.query(MarkovRelation, MarkovWord).\
                        filter(MarkovRelation.leader_id.in_(leader_ids.subquery())).\
                        join(MarkovWord, MarkovRelation.follower_id == MarkovWord.id):
                    follower_cache[word].setdefault(follower_word.word, 0)
                    follower_cache[word][follower_word.word] += 1

            word = choices(list(follower_cache[word].keys()),
                           weights=list(follower_cache[word].values()))[0]
            all_words.append(word)
        self.lock.release()
        return await ctx.send(' '.join(markov_word for markov_word in all_words))
