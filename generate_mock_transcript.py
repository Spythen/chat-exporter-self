import asyncio
import datetime
import os
from unittest.mock import MagicMock, AsyncMock
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

class MockMessage:
    def __init__(self, id, author, content, channel=None, created_at=None, reference=None, attachments=None, embeds=None, components=None):
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
        # Return an async generator/iterator yielding mock messages
        for msg in self.messages:
            yield msg

async def generate():
    # 1. Define mock users based on first/second person details
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

    # 2. Setup mock channel
    channel = MockChannel(
        id=1523165878609510471,
        name="3fxt-DM",
        recipient=u_3fxt
    )
    
    # 3. Build a conversation flow showing off V3 features
    messages = []
    
    # Msg 1: Initial introduction
    messages.append(MockMessage(
        id=10001,
        author=u_3fxt,
        content="Yo! Let's test the new MagCord V3 transcript system updates offline."
    ))
    
    # Msg 2: Reply
    ref = MockMessageReference(message_id=10001, channel_id=channel.id)
    reply_msg = MockMessage(
        id=10002,
        author=u_velocity,
        content="wsp! The mobile scaling on references is fixed now.",
        reference=ref
    )
    # Populate the resolved message cache inside the exporter
    reply_msg.reference.cached_message = messages[0]
    messages.append(reply_msg)
    
    # Msg 3: Text file attachment (tests document box and mobile preview hiding)
    txt_file = MockAttachment(
        filename="test_features.txt",
        url="https://raw.githubusercontent.com/Spythen/chat-exporter-self/main/README.md",
        size=142710,
        content_type="text/plain"
    )
    messages.append(MockMessage(
        id=10003,
        author=u_3fxt,
        content="Here is the features text file. The preview hides on phone viewports!",
        attachments=[txt_file]
    ))
    
    # Msg 4: Image attachment (tests lightbox and back button gestures)
    img_file = MockAttachment(
        filename="preview_image.png",
        url="https://cdn.discordapp.com/avatars/1236889131125051513/f19519bd7e7c8569aeed4f60b432d297.png?size=1024",
        size=88720,
        content_type="image/png"
    )
    # Force mock width/height to make it render as image instead of file box
    img_file.width = 1024
    img_file.height = 1024
    messages.append(MockMessage(
        id=10004,
        author=u_velocity,
        content="And check this image. Tap to pop up, double-finger pinch zoom works on phone!",
        attachments=[img_file]
    ))
    
    for msg in messages:
        msg.channel = channel
    channel.messages = messages

    # 4. Mock the bot client
    mock_bot = MagicMock()
    mock_bot.user = u_velocity
    
    print("Generating offline mock transcript...")
    transcript_html = await chat_exporter.export(
        channel=channel,
        bot=mock_bot
    )
    
    filename = "preview/transcript.html"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(transcript_html)
    print(f"Success! Mock preview transcript saved to: {os.path.abspath(filename)}")

if __name__ == "__main__":
    asyncio.run(generate())
