from bs4 import BeautifulSoup
from discord.ext import commands
from requests import get as requests_get

from discord_bot.cogs.common import CogHelper
from dappertable import shorten_string_cjk

BASE_URL = 'https://www.urbandictionary.com/'

class UrbanDictionary(CogHelper):
    '''
    Class that looks up urban dictionary definitions
    '''

    @commands.command(name='urban')
    async def word_lookup(self, ctx, *, word: str):
        '''
        Lookup word on urban dictionary

        search: str [Required]
            The word or phrase to search in urban dictionary
        '''
        if not await self.check_user_role(ctx):
            return await ctx.send('Unable to verify user role, ignoring command')
        self.logger.debug(f'Urban :: Looking up word string "{word}" {ctx.guild.id}')
        word_url = f'{BASE_URL}define.php?term={word}'
        result = requests_get(word_url, timeout=60)
        if result.status_code != 200:
            return await ctx.send(f'Unable to lookup word "{word}"')
        soup = BeautifulSoup(result.content, 'html.parser')
        definition_panels = soup.find_all("div", class_="definition")

        definitions = []
        for panel in definition_panels:
            meanings = panel.find_all('div', class_='meaning')
            for mean in meanings:
                definitions.append(mean.text)
        text = ''
        for (count, define) in enumerate(definitions[:2]):
            definition = shorten_string_cjk(define, 400)
            text = f'{text}{count}. {definition}\n'
        await ctx.send(f'```{text}```')
