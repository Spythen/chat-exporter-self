import sys
discord_modules = ['discord', 'nextcord', 'disnake', 'magcord']
loaded_modules = [m for m in discord_modules if m in sys.modules]
for m in loaded_modules:
    discord_modules.remove(m)
discord_modules = loaded_modules + discord_modules
for module in discord_modules:
    try:
        discord = __import__(module)
        discord.module = module
        discord_lib = module
        break
    except ImportError:
        continue

def _patch_discord_components():
    try:
        original_factory = discord.components._component_factory
        def my_component_factory(data, *args, **kwargs):
            comp = original_factory(data, *args, **kwargs)
            if comp is None:
                return data
            return comp
        discord.components._component_factory = my_component_factory
        if hasattr(discord, 'message') and hasattr(discord.message, '_component_factory'):
            discord.message._component_factory = my_component_factory
        
        old_handle = discord.Message._handle_components
        def _new_handle_components(self, data):
            self.components = []
            for component_data in data:
                component = my_component_factory(component_data, self)
                if component is not None:
                    self.components.append(component)
        discord.Message._handle_components = _new_handle_components
        print("DEBUG:", discord.Message._handle_components)
    except Exception as e:
        print(f"DEBUG PATCH ERROR: {e}")

_patch_discord_components()