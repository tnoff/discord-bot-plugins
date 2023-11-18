from re import search

from dappertable import DapperTable
from jsonschema import ValidationError

from discord.errors import NotFound
from discord.ext import commands
from discord_bot.cogs.common import CogHelper
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.utils import validate_config

# Role config schema
ROLE_SECTION_SCHEMA = {
    'type': 'object',
    'minProperties': 1,
    'additionalProperties': {
        'type': 'object',
        'properties': {
            'role_controls': {
                'type': 'object',
                'minProperties': 1,
                'additionalProperties': {
                    'type': 'object',
                    'properties': {
                        'controls': {
                            'type': 'array',
                            'minItems': 1,
                            'items': {
                                'type': 'integer',
                            },
                        },
                        'only_self': {
                            'type': 'boolean',
                            'default': False,
                        }
                    },
                    'required': [
                        'controls',
                    ]
                },
            },
            'reject_list': {
                'type': 'array',
                'items' : {
                    'type': 'integer',
                }
            },
            'require_role': {
                'type': 'integer',
            }
        }
    }
}

class RoleAssignment(CogHelper):
    '''
    Class that can add roles in more controlled fashion
    '''
    def __init__(self, bot, db_engine, logger, settings):
        super().__init__(bot, db_engine, logger, settings)
        self.logger = logger
        self.players = {}
        try:
            validate_config(settings['role'], ROLE_SECTION_SCHEMA)
        except ValidationError as exc:
            raise CogMissingRequiredArg('Unable to import roles due to invalid config') from exc

        self.settings = settings['role']

    @commands.group(name='role', invoke_without_command=False)
    async def role(self, ctx):
        '''
        Role functions.
        '''
        if ctx.invoked_subcommand is None:
            await ctx.send('Invalid sub command passed...')

    @role.command(name='list')
    async def role_list(self, ctx):
        '''
        List all roles in server
        '''
        headers = [
            {
                'name': 'Name',
                'length': 80,
            },
        ]
        table = DapperTable(headers, rows_per_message=15)
        for role in ctx.guild.roles:
            if role.id in self.settings[ctx.guild.id]['reject_list']:
                continue
            table.add_row([
                f'{role.name}'
            ])
        if table.size() == 0:
            return await ctx.send('No roles found')
        for item in table.print():
            await ctx.send(f'```{item}```')

    def get_controlled_roles(self, ctx, user=None):
        '''
        Get list of roles user controls
        '''
        controlled_roles = {}
        for role in ctx.author.roles:
            if role.id in self.settings[ctx.guild.id]['reject_list']:
                continue
            try:
                controls = self.settings[ctx.guild.id]['role_controls'][role.id]
            except KeyError:
                continue
            controls.setdefault('only_self', False)
            if controls['only_self'] and user:
                if ctx.author != user:
                    continue
            # We want to make sure if any of the controlled roles have only_self as false, we
            # set the value there to false
            for role_id in controls['controls']:
                control_role = ctx.guild.get_role(role_id)
                try:
                    existing_value = controlled_roles[control_role]
                    if existing_value is False:
                        continue
                except KeyError:
                    controlled_roles[control_role] = controls['only_self']
        return controlled_roles

    @role.command(name='controlled')
    async def role_controlled(self, ctx):
        '''
        List all roles in the server you can add
        '''
        headers = [
            {
                'name': 'Name',
                'length': 80,
            },
        ]
        table = DapperTable(headers, rows_per_message=15)
        for role, only_self in self.get_controlled_roles(ctx).items():
            out = f'{role.name}'
            if only_self:
                out = f'{out} (can only add/remove yourself from this role)'
            table.add_row([
                f'{out}'
            ])
        if table.size() == 0:
            return await ctx.send('No roles found')
        for item in table.print():
            await ctx.send(f'```{item}```')

    async def get_user_and_role(self, ctx, user, role):
        '''
        Get user and role objects
        '''
        try:
            user_id = int(search(r'\d+', user).group())
        except AttributeError:
            return None, None
        try:
            user = await ctx.guild.fetch_member(user_id)
        except NotFound:
            return None, None
        try:
            role_id = int(search(r'\d+', role).group())
        except AttributeError:
            return user, None
        try:
            role = ctx.guild.get_role(role_id)
        except NotFound:
            return user, None
        return user, role

    def check_required_role(self, ctx, user):
        '''
        Check user has required role before adding
        '''
        for role in user.roles:
            if role.id == self.settings[ctx.guild.id]['required_role']:
                return True
        return False

    @role.command(name='add')
    async def role_add(self, ctx, user, role):
        '''
        Add user to role that you control

        user [@mention]
            User you wish to add role to
        role [@mention]
            Role you wish to add that user to
        '''
        user_obj, role_obj = await self.get_user_and_role(ctx, user, role)
        if user_obj is None:
            return await ctx.send(f'Unable to find user {user}')
        if role_obj is None:
            return await ctx.send(f'Unable to find role {role}')
        controlled_roles = list(self.get_controlled_roles(ctx, user=user_obj).keys())
        user_name = user_obj.nick or user_obj.name
        if role_obj not in controlled_roles:
            return await ctx.send(f'Cannot add users to role {role_obj.name}, you do not control role. Use !role controlled to see a list of roles you control')
        if not self.check_required_role(ctx, user_obj):
            return await ctx.send(f'User {user_name} does not have required roles, skipping')
        if role_obj in user_obj.roles:
            return await ctx.send(f'User {user_name} already has role {role_obj.name}, skipping')
        await user_obj.add_roles(role_obj)
        return await ctx.send(f'Added user {user_name} to role {role_obj.name}')

    @role.command(name='remove')
    async def role_remove(self, ctx, user, role):
        '''
        Remove user to role that you control

        user [@mention]
            User you wish to remove role from
        role [@mention]
            Role you wish to remove that user from
        '''
        user_obj, role_obj = await self.get_user_and_role(ctx, user, role)
        if user_obj is None:
            return await ctx.send(f'Unable to find user {user}')
        if role_obj is None:
            return await ctx.send(f'Unable to find role {role}')
        controlled_roles = list(self.get_controlled_roles(ctx, user=user_obj).keys())
        user_name = user_obj.nick or user_obj.name
        if role_obj not in controlled_roles:
            return await ctx.send(f'Cannot remove users to role {role_obj.name}, you do not control role. Use !role controlled to see a list of roles you control')
        if role_obj not in user_obj.roles:
            return await ctx.send(f'User {user_name} does not have role {role_obj.name}, skipping')
        await user_obj.remove_roles(role_obj)
        return await ctx.send(f'Removed user {user_name} from role {role_obj.name}')
