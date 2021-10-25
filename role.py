import asyncio

from discord.ext import commands
from discord.errors import NotFound
from sqlalchemy import Column, Integer, String
from sqlalchemy import ForeignKey

from discord_bot.cogs.common import CogHelper
from discord_bot.database import BASE

EMOJI_MAPPING = {
    '\u0030\ufe0f\u20e3': ':zero:',
    '\u0031\ufe0f\u20e3': ':one:',
    '\u0032\ufe0f\u20e3': ':two:',
    '\u0033\ufe0f\u20e3': ':three:',
    '\u0034\ufe0f\u20e3': ':four:',
    '\u0035\ufe0f\u20e3': ':five:',
    '\u0036\ufe0f\u20e3': ':six:',
    '\u0037\ufe0f\u20e3': ':seven:',
    '\u0038\ufe0f\u20e3': ':eight:',
    '\u0039\ufe0f\u20e3': ':nine:',
}

NUMBER_DICT = {
    1: 'one',
    2: 'two',
    3: 'three',
    4: 'four',
    5: 'five',
    6: 'six',
    7: 'seven',
    8: 'eight',
    9: 'nine',
    0: 'zero',
}

#
# Role Assignment Tables
#

class RoleAssignmentMessage(BASE):
    '''
    Message for role assignment
    '''
    __tablename__ = 'role_assignment_message'
    id = Column(Integer, primary_key=True)
    message_id = Column(String(128))
    channel_id = Column(String(128))
    server_id = Column(String(128))

class RoleAssignmentReaction(BASE):
    '''
    Emoji and Role Association
    '''
    __tablename__ = 'role_assignment_reaction'
    id = Column(Integer, primary_key=True)
    role_id = Column(String(128))
    emoji_name = Column(String(64))
    role_assignment_message_id = Column(Integer, ForeignKey('role_assignment_message.id'))

class RoleAssignment(CogHelper):
    '''
    Function to add message users can react to get assignment.
    Also includes loop that will check for new role assignment messages every 5 minutes
    '''
    def __init__(self, bot, db_session, logger, settings):
        super().__init__(bot, db_session, logger, settings)
        self.bot.loop.create_task(self.player_loop())

    async def player_loop(self):
        '''
        Our main player loop.
        '''
        await self.bot.wait_until_ready()

        message_cache = {}
        role_cache = {}


        # TODO delete messages after some amount of time


        while not self.bot.is_closed():
            # Go through each saved message in database
            # Save any you should delete
            will_delete = []
            for assignment_message in self.db_session.query(RoleAssignmentMessage).all():
                self.logger.info(f'Checking assignment message {assignment_message.id}')
                guild = await self.bot.fetch_guild(int(assignment_message.server_id))
                try:
                    message = message_cache[assignment_message.message_id]
                except KeyError:
                    channel = self.bot.get_channel(int(assignment_message.channel_id))
                    try:
                        message = await channel.fetch_message(int(assignment_message.message_id))
                        message_cache[assignment_message.message_id] = message
                    except NotFound:
                        self.logger.error(f'Unable to find message {assignment_message.id}'
                                          ' going to delete db entry')
                        will_delete.append(assignment_message)
                        continue

                # Get mapping of what reactions should go with which role
                reaction_dict = {}
                for role_reaction in self.db_session.query(RoleAssignmentReaction).\
                    filter(RoleAssignmentReaction.role_assignment_message_id == assignment_message.id): #pylint:disable=line-too-long
                    reaction_dict[role_reaction.emoji_name] = role_reaction.role_id


                # Find reactions to the mssage
                for reaction in message.reactions:
                    self.logger.debug(f'Checking reaction {reaction} ' \
                                 f'for message {assignment_message.id}')

                    # Get role reaction mapping
                    role_id = reaction_dict[EMOJI_MAPPING[reaction.emoji]]
                    try:
                        role = role_cache[role_id]
                    except KeyError:
                        role = guild.get_role(int(role_id))
                        role_cache[role_id] = role

                    # Check for users in reaction
                    async for user in reaction.users():
                        member = await guild.fetch_member(int(user.id))
                        if not member:
                            self.logger.error(f'Unable to read member for user {user.id} '\
                                              f'in guild {guild.id}, likely a permissions issue')
                            continue
                        if role not in member.roles:
                            await member.add_roles(role)
                            self.logger.info(f'Adding role {role.name} to user {user.name}')

            # Delete all messages we could not find earlier
            for assignment_message in will_delete:
                # Delete reactions first
                self.db_session.query(RoleAssignmentReaction).\
                    filter(RoleAssignmentReaction.role_assignment_message_id == assignment_message.id).delete() #pylint:disable=line-too-long
                self.db_session.delete(assignment_message)
                self.db_session.commit()
            await asyncio.sleep(300)

    @commands.command(name='assign-roles')
    async def roles(self, ctx):
        '''
        Generate message with all roles.
        Users can reply to this message to add roles to themselves.
        '''
        self.logger.debug(f'Setting up message for role grants in server {ctx.guild.id}')
        index = 0
        message_strings = []
        message_string = 'React with the following emojis to be automatically granted roles'
        role_assign_list = []
        for role in ctx.guild.roles:
            # Ignore everyone role
            if role.name == '@everyone':
                continue
            # Only allow roles with no extra permissions
            if role.permissions.value != 0:
                continue
            emoji = f':{NUMBER_DICT[index]}:'
            message_string = f'{message_string}\nFor role `@{role.name}`'
            message_string = f'{message_string} reply with emoji {emoji}'
            role_assign_list.append({'role_id': role.id, 'emoji_name': emoji})
            index += 1
            # Only show 10 roles at a time, since we only have 10 emojis to works with
            if index >= 9:
                index = 0
                message_strings.append(message_string)
                message_string = 'React with the following emojis to' \
                                 'be automatically granted roles'

        message_strings.append(message_string)
        for message_string in message_strings:

            message = await ctx.send(f'{message_string}')
            new_message = RoleAssignmentMessage(message_id=str(message.id),
                                                channel_id=str(message.channel.id),
                                                server_id=str(message.guild.id))
            self.db_session.add(new_message)
            self.db_session.commit()
            self.logger.info(f'Created new role assignment message {new_message.id}')
            for role_assign in role_assign_list:
                role_assign['role_assignment_message_id'] = new_message.id
                assignment = RoleAssignmentReaction(**role_assign)
                self.db_session.add(assignment)
                self.db_session.commit()
                self.logger.info(f'Created new role assignment reaction {assignment.id}')
