import asyncio
import random
import re
import typing

from discord import TextChannel
from discord.ext import commands
from discord.errors import NotFound
from sqlalchemy import Boolean, Column, Integer, String
from sqlalchemy import ForeignKey, UniqueConstraint

from discord_bot.cogs.common import CogHelper
from discord_bot.database import BASE

# Get this number of random items to keep in memory when running markov speak
MARKOV_RANDOM_SPLIT = 10

def clean_message(content, emoji_ids):
    '''
    Clean channel message
    content :   Full message content to clean
    emojis  :   List of server emoji ids, so we can remove any not from server

    Returns "corpus", list of cleaned words
    '''
    # Remove web links and mentions from text
    message_text = re.sub(r'(https?\://|\<\@)\S+', '',
                          content, flags=re.MULTILINE)
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
        match = re.match('^\ *<(?P<emoji>:\w+:)(?P<id>\d+)>\ *$', word)
        if match:
            if int(match.group('id')) in emoji_ids:
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
    __table_args__ = (
        UniqueConstraint('leader_id', 'follower_id',
                         name='_unique_markov_relation'),
    )
    id = Column(Integer, primary_key=True)
    leader_id = Column(Integer, ForeignKey('markov_word.id'))
    follower_id = Column(Integer, ForeignKey('markov_word.id'))
    count = Column(Integer)


class Markov(CogHelper):
    '''
    Save markov relations to a database periodically
    '''
    def __init__(self, bot, db_session, logger, settings):
        super().__init__(bot, db_session, logger, settings)
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

    def __ensure_relation(self, leader, follower):
        markov_relation = self.db_session.query(MarkovRelation).\
                filter(MarkovRelation.leader_id == leader.id).\
                filter(MarkovRelation.follower_id == follower.id).first()
        if markov_relation:
            return markov_relation
        new_relation = MarkovRelation(leader_id=leader.id,
                                      follower_id=follower.id,
                                      count=0)
        self.db_session.add(new_relation)
        self.db_session.commit()
        self.db_session.flush()
        return new_relation

    # https://srome.github.io/Making-A-Markov-Chain-Twitter-Bot-In-Python/
    def __build_and_save_relations(self, corpus, markov_channel):
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
            relation = self.__ensure_relation(leader_word, follower_word)
            relation.count += 1

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
            for markov_channel in self.db_session.query(MarkovChannel).all():
                channel = await self.bot.fetch_channel(markov_channel.channel_id)
                server = await self.bot.fetch_guild(markov_channel.server_id)
                emoji_ids = [emoji.id for emoji in await server.fetch_emojis()]
                self.logger.info('Gathering markov messages for '
                                 f'channel {markov_channel.channel_id}')
                # Start at the beginning of channel history,
                # slowly make your way make to current day
                if not markov_channel.last_message_id:
                    messages = await channel.history(limit=128, oldest_first=True).flatten()
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
                    self.__build_and_save_relations(corpus, markov_channel)
                # Commit at the end in case the last message was skipped
                self.db_session.commit()

            await asyncio.sleep(180)

    @commands.group(name='markov', invoke_without_command=False)
    async def markov(self, ctx): #pylint:disable=no-self-use
        '''
        Markov functions
        '''
        if ctx.invoked_subcommand is None:
            await ctx.send('Invalid sub command passed...')

    @markov.command(name='on')
    async def on(self, ctx, channel_type: typing.Optional[str] = 'public'):
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

        return await ctx.send('Markov turned off for channel')

    @markov.command(name='speak')
    async def speak(self, ctx, #pylint:disable=too-many-locals
                    first_word: typing.Optional[str] = '',
                    sentence_length: typing.Optional[int] = 32):
        '''
        Say a random sentence generated by markov

        Note that this uses all markov channels setup for the server

        first_word  :   First word for markov string, if not given will be random.
        sentence_length :   Length of sentence

        Note that for first_word, multiple words can be given, but they must be in quotes
        Ex: !markov speak "hey whats up", or !markov speak "hey whats up" 64
        '''

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
        word = self.db_session.query(MarkovWord).get(random.choice(possible_word_ids)).word
        all_words.append(word)

        # Save a cache layer to reduce db calls
        follower_cache = {}
        for _ in range(sentence_length + 1):
            try:
                _follower_choices = follower_cache[word]['choices']
                _follower_weights = follower_cache[word]['weights']
            except KeyError:
                follower_cache[word] = {'choices' : [], 'weights': []}

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

                # Get total amount of leader and followers
                total_followers = self.db_session.query(MarkovRelation, MarkovWord).\
                        filter(MarkovRelation.leader_id.in_(leader_ids.subquery())).\
                        join(MarkovWord, MarkovRelation.follower_id == MarkovWord.id).count()

                # Instead of gathering every possible outcome, which can lead to thousands of entries in memory
                # .. grab a random value from a portion from every Nth split ( set by MARKOV_RANDOM_SPLIT ) portion of the list, sorted
                # .. by relation count
                # For example, if you had 100 entries and a split of 10, grab a random value from every 10th portion
                # Random value from 0 --> 9 position
                # Random value from 10 --> 19 position
                # Random value from 20 --> 29 position
                # etc..

                # First generate basic ordered query to get the offsets from
                follower_relation_query = self.db_session.query(MarkovRelation, MarkovWord).\
                        filter(MarkovRelation.leader_id.in_(leader_ids.subquery())).\
                        join(MarkovWord, MarkovRelation.follower_id == MarkovWord.id).\
                        order_by(MarkovRelation.count.asc())

                # Then grab each value from the Nth positions
                current_index = 0
                while current_index < total_followers:
                    max_range = current_index + (int(total_followers / MARKOV_RANDOM_SPLIT) or 1)
                    random_offset = random.randint(current_index, max_range - 1)
                    # Usually typeerror here means we ran out of results
                    try:
                        relation, follower = follower_relation_query.offset(random_offset).first()
                    except TypeError:
                        self.logger.warning(f'Type Error on sql query, offset {random_offset}, leader word {word}')
                        break
                    follower_cache[word]['choices'].append(follower.word)
                    follower_cache[word]['weights'].append(relation.count)
                    current_index = max_range
            word = random.choices(follower_cache[word]['choices'],
                                  weights=follower_cache[word]['weights'],
                                  k=1)[0]
            all_words.append(word)
        return await ctx.send(' '.join(markov_word for markov_word in all_words))
