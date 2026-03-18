discord_modules = ['nextcord', 'disnake', 'magcord', 'discord']
for module in discord_modules:
    try:
        discord = __import__(module)
        discord.module = module
        break
    except ImportError:
        continue
