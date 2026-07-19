import asyncio
import datetime
import os
from unittest.mock import MagicMock
import discord
import chat_exporter

# Mock classes to simulate discord.py structures offline
class MockAsset:
    def __init__(self, url):
        self.url = url
    def __str__(self):
        return self.url

class MockUser:
    def __init__(self, id, name, display_name, avatar_url, created_at=None, bot=False):
        self.id = id
        self.name = name
        self.display_name = display_name
        self.global_name = display_name
        self.discriminator = "0000"
        self.avatar = MockAsset(avatar_url)
        self.display_avatar = self.avatar
        self.created_at = created_at or datetime.datetime.now(datetime.timezone.utc)
        self.bot = bot
        self.color = discord.Color.default()
        self.roles = []
        self.top_role = None

class MockAttachment:
    def __init__(self, filename, url, size, content_type="application/octet-stream"):
        self.filename = filename
        self.url = url
        self.size = size
        self.content_type = content_type
        self.proxy_url = url
        self.height = None
        self.width = None

    async def read(self):
        return b"MagCord V3 Transcript System\n- Supporting offline preview generation!\n- Optimized for PC and mobile layout.\n"

class MockMessageReference:
    def __init__(self, message_id, channel_id, guild_id=None):
        self.message_id = message_id
        self.channel_id = channel_id
        self.guild_id = guild_id

class MockMessageSnapshot:
    def __init__(self, content):
        self.content = content

class MockMessage:
    def __init__(self, id, author, content, channel=None, created_at=None, reference=None, attachments=None, embeds=None, components=None, snapshots=None):
        self.id = id
        self.author = author
        self.content = content
        self.clean_content = content
        self.channel = channel
        self.created_at = created_at or datetime.datetime.now(datetime.timezone.utc)
        self.edited_at = None
        self.reference = reference
        self.attachments = attachments or []
        self.embeds = embeds or []
        self.components = components or []
        self.snapshots = snapshots or []
        self.reactions = []
        self.pinned = False
        self.type = discord.MessageType.default
        self.interaction = None
        self.stickers = []
        self.flags = MagicMock()
        self.flags.value = 0

class MockChannel:
    def __init__(self, id, name, recipient=None):
        self.id = id
        self.name = name
        self.recipient = recipient
        self.type = discord.ChannelType.private if recipient else discord.ChannelType.text
        self.messages = []
        self.guild = None
        self.created_at = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=365)

    async def history(self, limit=None, oldest_first=True, **kwargs):
        for msg in self.messages:
            yield msg

# Create standard mock users
u_3fxt = MockUser(
    id=1464908071729102878,
    name="3fxt",
    display_name="Mr Velocity",
    avatar_url="https://cdn.discordapp.com/avatars/1464908071729102878/a_51d5bd75bf2a123cce3cd7ef48a74cdc.gif?size=1024",
    created_at=datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=175)
)

u_velocity = MockUser(
    id=1236889131125051513,
    name="therealvelocity",
    display_name="! IShowVelocity",
    avatar_url="https://cdn.discordapp.com/avatars/1236889131125051513/f19519bd7e7c8569aeed4f60b432d297.png?size=1024",
    created_at=datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=804)
)

mock_bot = MagicMock()
mock_bot.user = u_velocity

async def save_transcript(channel, num, theme):
    print(f"Generating transcript_{num}.html ({theme} theme)...")
    transcript_html = await chat_exporter.export(
        channel=channel,
        bot=mock_bot
    )
    # Customize the default theme inside generated HTML
    old_theme_logic = "savedTheme = localStorage.getItem('chat-exporter-theme') || 'dark';"
    new_theme_logic = f"savedTheme = localStorage.getItem('chat-exporter-theme') || '{theme}';"
    transcript_html = transcript_html.replace(old_theme_logic, new_theme_logic)

    filename = f"preview/transcript_{num}.html"
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w", encoding="utf-8") as f:
        f.write(transcript_html)
    print(f"Success! Saved to: {os.path.abspath(filename)}")

async def generate():
    # ----------------------------------------------------
    # Transcript 1: Dark Theme - General Chat, Replies, Mentions, Simple Embed
    # ----------------------------------------------------
    ch1 = MockChannel(id=111, name="general-chat")
    msgs1 = []
    msgs1.append(MockMessage(id=101, author=u_3fxt, content="Yo! Welcome to the first V3 transcript preview."))
    
    ref = MockMessageReference(message_id=101, channel_id=ch1.id)
    reply = MockMessage(id=102, author=u_velocity, content="Thanks! Replying to test font scaling ratios.", reference=ref)
    reply.reference.cached_message = msgs1[0]
    msgs1.append(reply)
    
    # Mention example
    msgs1.append(MockMessage(id=103, author=u_3fxt, content="Check this out <@1236889131125051513> ! The mentions look clean."))
    
    # Simple Embed
    embed1 = discord.Embed(title="MagCord Updates", description="Welcome to version 3!", color=discord.Color.blue())
    embed1.set_author(name="System Bot", icon_url=u_velocity.avatar.url)
    msgs1.append(MockMessage(id=104, author=u_velocity, content="", embeds=[embed1]))

    for m in msgs1: m.channel = ch1
    ch1.messages = msgs1
    await save_transcript(ch1, 1, "dark")

    # ----------------------------------------------------
    # Transcript 2: Light Theme - Multiple Attachments (File lists, Image lightbox, Video preview)
    # ----------------------------------------------------
    ch2 = MockChannel(id=222, name="media-and-files")
    msgs2 = []
    
    # File attachment
    txt_file = MockAttachment(filename="config_details.json", url="https://raw.githubusercontent.com/Spythen/chat-exporter-self/main/pyproject.toml", size=1412, content_type="application/json")
    msgs2.append(MockMessage(id=201, author=u_3fxt, content="Here's a small configuration file:", attachments=[txt_file]))
    
    # Image attachment (for Lightbox preview test)
    img_file = MockAttachment(filename="avatar_preview.png", url=u_velocity.avatar.url, size=105739, content_type="image/png")
    img_file.width = 512
    img_file.height = 512
    msgs2.append(MockMessage(id=202, author=u_velocity, content="Check out my avatar in full screen:", attachments=[img_file]))

    # Video attachment
    video_file = MockAttachment(filename="demo_video.mp4", url="https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/ForBiggerEscapes.mp4", size=2048576, content_type="video/mp4")
    video_file.width = 640
    video_file.height = 360
    msgs2.append(MockMessage(id=203, author=u_3fxt, content="Here is a sample mp4 video attachment:", attachments=[video_file]))

    for m in msgs2: m.channel = ch2
    ch2.messages = msgs2
    await save_transcript(ch2, 2, "light")

    # ----------------------------------------------------
    # Transcript 3: Onyx Theme - Discord Components (Buttons, Select Menus, Separators)
    # ----------------------------------------------------
    ch3 = MockChannel(id=333, name="interactive-components")
    msgs3 = []

    # Container with Buttons (type 2)
    buttons_row = {
        "type": 1,
        "components": [
            {"type": 2, "style": 1, "label": "Accept", "custom_id": "accept_btn"},
            {"type": 2, "style": 3, "label": "Configure", "custom_id": "config_btn"},
            {"type": 2, "style": 4, "label": "Decline", "custom_id": "decline_btn"},
            {"type": 2, "style": 5, "label": "Link Out", "url": "https://github.com/Spythen/chat-exporter-self"}
        ]
    }
    msgs3.append(MockMessage(id=301, author=u_velocity, content="Please interact with the button component panel below:", components=[buttons_row]))

    # Select Menu (type 3)
    select_row = {
        "type": 1,
        "components": [
            {
                "type": 3,
                "custom_id": "select_theme",
                "placeholder": "Choose your default theme...",
                "options": [
                    {"label": "Dark", "value": "dark", "description": "Standard dark mode layout"},
                    {"label": "Light", "value": "light", "description": "Bright and clean look"},
                    {"label": "Onyx", "value": "onyx", "description": "Super dark pitch black look"}
                ]
            }
        ]
    }
    msgs3.append(MockMessage(id=302, author=u_3fxt, content="Or choose an option from the select dropdown component:", components=[select_row]))

    # Separator (type 14) and container
    separator_comp = {
        "type": 17,
        "accent_color": 9983,
        "components": [
            {"type": 10, "content": "**System Status: All systems operational.**"},
            {"type": 14, "spacing": 1, "divider": True},
            {"type": 10, "content": "-# Last updated: just now"}
        ]
    }
    msgs3.append(MockMessage(id=303, author=u_velocity, content="Status panel container details:", components=[separator_comp]))

    for m in msgs3: m.channel = ch3
    ch3.messages = msgs3
    await save_transcript(ch3, 3, "onyx")

    # ----------------------------------------------------
    # Transcript 4: Ash Theme - Forwarded Messages & Message Snapshots
    # ----------------------------------------------------
    ch4 = MockChannel(id=444, name="forwarded-messages")
    msgs4 = []

    # Forwarded snapshot content
    fwd_msg = MockMessageSnapshot(content="Hey! This is the original message that is being forwarded.")
    msgs4.append(MockMessage(
        id=401,
        author=u_3fxt,
        content="Check this message I forwarded from another channel:",
        snapshots=[fwd_msg]
    ))

    # Multiple forwarded messages in a row
    fwd_msg2 = MockMessageSnapshot(content="Second message context being forwarded.")
    msgs4.append(MockMessage(
        id=402,
        author=u_velocity,
        content="I also forwarded this details:",
        snapshots=[fwd_msg2]
    ))

    for m in msgs4: m.channel = ch4
    ch4.messages = msgs4
    await save_transcript(ch4, 4, "ash")

    # ----------------------------------------------------
    # Transcript 5: Onyx Theme - Complex Hybrid Chat (Mentions, Reactions, Fields Embeds, Code blocks)
    # ----------------------------------------------------
    ch5 = MockChannel(id=555, name="developer-sandbox")
    msgs5 = []

    # Embed with fields and author/footer
    embed5 = discord.Embed(title="System Performance Metrics", description="Server status details:", color=discord.Color.green())
    embed5.set_author(name="V3 Performance monitor", icon_url=u_velocity.avatar.url)
    embed5.add_field(name="CPU Load", value="`12.4%` (Low)", inline=True)
    embed5.add_field(name="RAM Usage", value="`2.1 GB / 16.0 GB`", inline=True)
    embed5.add_field(name="Ping", value="`14 ms`", inline=True)
    embed5.set_footer(text="Verified metrics status", icon_url=u_3fxt.avatar.url)
    
    msgs5.append(MockMessage(id=501, author=u_velocity, content="Here are the live metrics:", embeds=[embed5]))

    # Code block message
    code_block_content = "```python\ndef greet(name):\n    print(f'Hello, {name}!')\n\n# Run local test\ngreet('Mr Velocity')\n```"
    msgs5.append(MockMessage(id=502, author=u_3fxt, content=code_block_content))

    # Reactions on a message
    reaction_msg = MockMessage(id=503, author=u_velocity, content="If you like these V3 improvements, react to this message!")
    
    # Custom reaction mock structures
    rx1 = MagicMock()
    rx1.emoji = "🔥"
    rx1.count = 5
    rx1.me = False
    
    rx2 = MagicMock()
    rx2.emoji = "🚀"
    rx2.count = 3
    rx2.me = True
    
    reaction_msg.reactions = [rx1, rx2]
    msgs5.append(reaction_msg)

    for m in msgs5: m.channel = ch5
    ch5.messages = msgs5
    await save_transcript(ch5, 5, "onyx")

if __name__ == "__main__":
    asyncio.run(generate())
