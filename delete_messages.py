from asyncio import sleep
from datetime import datetime, timedelta

from discord.errors import HTTPException, DiscordServerError
from jsonschema import ValidationError
from pytz import UTC

from discord_bot.cogs.common import CogHelper
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.utils import async_retry_command, retry_command, validate_config

# Default for deleting messages after X days
DELETE_AFTER_DEFAULT = 7

# Default for how to wait between each loop
LOOP_SLEEP_INTERVAL_DEFAULT = 300

DELETE_MESSAGES_SCHEMA  = {
    'type': 'object',
    'properties': {
        'loop_sleep_interval': {
            'type': 'number',
        },
        'discord_channels': {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'server_id': {
                        'type': 'integer',
                    },
                    'channel_id': {
                        'type': 'integer'
                    },
                    'delete_after': {
                        'type': 'integer'
                    },
                },
                'required': [
                    'server_id',
                    'channel_id',
                ]
            }
        },
    },
    'required': [
        'discord_channels',
    ],
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

class DeleteMessages(CogHelper):
    '''
    Delete Messages in Channels after X days
    '''
    def __init__(self, bot, db_engine, logger, settings):
        super().__init__(bot, db_engine, logger, settings)
        try:
            validate_config(settings['delete_messages'], DELETE_MESSAGES_SCHEMA)
        except ValidationError as exc:
            raise CogMissingRequiredArg('Invalid config given for delete messages') from exc
        except KeyError:
            settings['delete_messages'] = {}
        self.loop_sleep_interval = settings['delete_messages'].get('loop_sleep_interval', LOOP_SLEEP_INTERVAL_DEFAULT)
        self.discord_channels = settings['delete_messages'].get('discord_channels', [])
        self._task = None

    async def cog_load(self):
        self._task = self.bot.loop.create_task(self.main_loop())

    async def cog_unload(self):
        if self._task:
            self._task.cancel()

    async def main_loop(self):
        '''
        Our main loop.
        '''
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():
            try:
                await self.__main_loop()
            except Exception as e:
                self.logger.exception(e)
                print(f'Player loop exception {str(e)}')

    async def __main_loop(self):
        '''
        Main loop runner
        '''
        for channel_dict in self.discord_channels:
            await sleep(.01)
            self.logger.debug(f'Delete Messages :: Checking Channel ID {channel_dict["channel_id"]}')
            channel = await async_retry_discord_message_command(self.bot.fetch_channel, channel_dict["channel_id"])

            delete_after = channel_dict.get('delete_after', DELETE_AFTER_DEFAULT)
            cutoff_period = (datetime.utcnow() - timedelta(days=delete_after)).replace(tzinfo=UTC)
            messages = [m async for m in retry_discord_message_command(channel.history, limit=128, oldest_first=True)]
            for message in messages:
                if message.created_at < cutoff_period:
                    self.logger.info(f'Deleting message id {message.id}, in channel {channel.id}, in server {channel_dict["server_id"]}')
                    await async_retry_discord_message_command(message.delete)
        await sleep(self.loop_sleep_interval)
