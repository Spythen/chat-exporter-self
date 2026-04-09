import html
import io
import re
import traceback
from typing import List, Optional, Union

import aiohttp
from pytz import timezone
from datetime import timedelta

from chat_exporter.construct.attachment_handler import AttachmentHandler
from chat_exporter.ext.discord_import import discord

from chat_exporter.construct.assets import Attachment, Component, Embed, Reaction
from chat_exporter.ext.discord_utils import DiscordUtils
from chat_exporter.ext.discriminator import discriminator
from chat_exporter.ext.cache import cache
from chat_exporter.ext.html_generator import (
    fill_out,
    bot_tag,
    bot_tag_verified,
    message_body,
    message_pin,
    message_thread,
    message_content,
    message_reference,
    message_reference_unknown,
    message_reference_forwarded,
    message_interaction,
    img_attachment,
    start_message,
    end_message,
    PARSE_MODE_NONE,
    PARSE_MODE_MARKDOWN,
    PARSE_MODE_REFERENCE,
    message_thread_remove,
    message_thread_add,
)


def _gather_user_bot(author: discord.Member):
    if author.bot and author.public_flags.verified_bot:
        return bot_tag_verified
    elif author.bot:
        return bot_tag
    return ""


def _set_edit_at(message_edited_at):
    return f'<span class="chatlog__reference-edited-timestamp" data-timestamp="{message_edited_at}">(edited)</span>'


class MessageConstruct:
    message_html: str = ""

    # Asset Types
    embeds: str = ""
    reactions: str = ""
    components: str = ""
    attachments: str = ""
    time_format: str = ""

    interaction: str = ""

    def __init__(
        self,
        message: discord.Message,
        previous_message: Optional[discord.Message],
        pytz_timezone,
        military_time: bool,
        guild: discord.Guild,
        meta_data: dict,
        message_dict: dict,
        attachment_handler: Optional[AttachmentHandler],
        bot: Optional[discord.Client] = None
    ):
        self.message = message
        self.previous_message = previous_message
        self.pytz_timezone = pytz_timezone
        self.military_time = military_time
        self.guild = guild
        self.message_dict = message_dict
        self.attachment_handler = attachment_handler
        self.bot = bot
        self.time_format = "%A, %e %B %Y %I:%M %p"
        if self.military_time:
            self.time_format = "%A, %e %B %Y %H:%M"

        self.message_created_at, self.message_edited_at = self.set_time()
        self.meta_data = meta_data
        self.forwarded = False

    def get_message_snapshots(self):
        if hasattr(self.message, "message_snapshots"):
            return self.message.message_snapshots
        elif hasattr(self.message, "snapshots"):
            return self.message.snapshots
        return []

    @staticmethod
    def _collect_attachment_urls(attachment):
        urls = set()
        for attr in ("url", "proxy_url"):
            value = getattr(attachment, attr, None)
            if value:
                urls.add(str(value))
        return urls

    @staticmethod
    def _embed_has_non_image_content(embed) -> bool:
        if getattr(embed, "title", None):
            return True
        if getattr(embed, "description", None):
            return True
        if getattr(embed, "fields", None):
            if len(embed.fields) > 0:
                return True
        author = getattr(embed, "author", None)
        if author and getattr(author, "name", None):
            return True
        footer = getattr(embed, "footer", None)
        if footer and getattr(footer, "text", None):
            return True
        thumbnail = getattr(embed, "thumbnail", None)
        if thumbnail and getattr(thumbnail, "url", None):
            return True
        return False

    def _is_duplicate_image_embed(self, embed, attachment_urls) -> bool:
        if not attachment_urls:
            return False
        image = getattr(embed, "image", None)
        if not image:
            return False
        image_url = getattr(image, "proxy_url", None) or getattr(image, "url", None)
        if not image_url or str(image_url) not in attachment_urls:
            return False
        if self._embed_has_non_image_content(embed):
            return False
        return True

    async def construct_message(
        self,
    ) -> (str, dict):
        if discord.MessageType.pins_add == self.message.type:
            await self.build_pin()
        elif discord.MessageType.thread_created == self.message.type:
            await self.build_thread()
        elif discord.MessageType.recipient_remove == self.message.type:
            await self.build_thread_remove()
        elif discord.MessageType.recipient_add == self.message.type:
            await self.build_thread_add()
        else:
            await self.build_message()
        return self.message_html, self.meta_data

    async def build_message(self):
        await self.build_content()
        await self.build_reference()
        await self.build_interaction()
        await self.build_sticker()
        await self.build_assets()
        await self.build_message_template()
        await self.build_meta_data()

    async def build_pin(self):
        await self.generate_message_divider(channel_audit=True)
        await self.build_pin_template()

    async def build_thread(self):
        await self.generate_message_divider(channel_audit=True)
        await self.build_thread_template()

    async def build_thread_remove(self):
        await self.generate_message_divider(channel_audit=True)
        await self.build_remove()

    async def build_thread_add(self):
        await self.generate_message_divider(channel_audit=True)
        await self.build_add()

    async def build_meta_data(self):
        user_id = self.message.author.id

        if user_id in self.meta_data:
            self.meta_data[user_id][4] += 1
        else:
            user_name_discriminator = await discriminator(self.message.author.name, self.message.author.discriminator)
            user_created_at = self.message.author.created_at
            user_bot = _gather_user_bot(self.message.author)
            user_avatar = (
                self.message.author.display_avatar if self.message.author.display_avatar
                else DiscordUtils.default_avatar
            )
            user_joined_at = self.message.author.joined_at if hasattr(self.message.author, "joined_at") else None
            user_display_name = (
                f'<div class="meta__display-name">{self.message.author.display_name}</div>'
                if self.message.author.display_name != self.message.author.name
                else ""
            )
            self.meta_data[user_id] = [
                user_name_discriminator, user_created_at, user_bot, user_avatar, 1, user_joined_at, user_display_name
            ]

    async def build_content(self):
        if self.message_edited_at:
            self.message_edited_at = _set_edit_at(self.message_edited_at)

        snapshots = self.get_message_snapshots()
        if snapshots:
            snapshot_content = []
            for s in snapshots:
                if hasattr(s, 'content') and s.content:
                    snapshot_content.append(f'<div class="chatlog__markdown-preserve">{html.escape(s.content)}</div>')
            
            combined = html.escape(self.message.content or "")
            if snapshot_content:
                combined += f'<div class="chatlog__forwarded-snapshot">{"".join(snapshot_content)}</div>'
            self.forwarded = True
        else:
            combined = html.escape(self.message.content or "")

        # Ensure content is at least an empty string to avoid rendering issues
        self.message.content = await fill_out(self.guild, message_content, [
            ("MESSAGE_CONTENT", combined, PARSE_MODE_MARKDOWN),
            ("EDIT", self.message_edited_at, PARSE_MODE_NONE),
        ], bot=self.bot)

    async def build_reference(self):
        if not self.message.reference:
            self.message.reference = ""
            return

        message: discord.Message = self.message_dict.get(self.message.reference.message_id)

        if not message:
            try:
                message: discord.Message = await self.message.channel.fetch_message(self.message.reference.message_id)
            except Exception:
                self.message.reference = ""
                if self.forwarded:
                    self.message.reference = await fill_out(self.guild, message_reference_forwarded, [
                        ("FORWARD_ICON", DiscordUtils.forward_icon, PARSE_MODE_NONE),
                    ])
                    return
                self.message.reference = message_reference_unknown
                return

        is_bot = _gather_user_bot(message.author)
        user_colour = await self._gather_user_colour(message.author)

        icon = ""
        dummy = ""
        def get_interaction_status(interaction_message):
            if hasattr(interaction_message, 'interaction_metadata'):
                return interaction_message.interaction_metadata
            return interaction_message.interaction

        interaction_status = get_interaction_status(message)
        if not interaction_status and (message.embeds or message.attachments):
            icon = DiscordUtils.reference_attachment_icon
            dummy = "Click to see attachment"
        elif interaction_status:
            icon = DiscordUtils.interaction_command_icon
            dummy = "Click to see command"

        if not message.content:
            message.content = dummy

        _, message_edited_at = self.set_time(message)

        if message_edited_at:
            message_edited_at = _set_edit_at(message_edited_at)

        avatar_url = message.author.display_avatar if message.author.display_avatar else DiscordUtils.default_avatar
        self.message.reference = await fill_out(self.guild, message_reference, [
            ("AVATAR_URL", str(avatar_url), PARSE_MODE_NONE),
            ("BOT_TAG", is_bot, PARSE_MODE_NONE),
            ("NAME_TAG", await discriminator(message.author.name, message.author.discriminator), PARSE_MODE_NONE),
            ("NAME", str(html.escape(message.author.display_name))),
            ("USER_COLOUR", user_colour, PARSE_MODE_NONE),
            ("CONTENT", message.content.replace("\n", "").replace("<br>", ""), PARSE_MODE_REFERENCE),
            ("EDIT", message_edited_at, PARSE_MODE_NONE),
            ("ICON", icon, PARSE_MODE_NONE),
            ("USER_ID", str(message.author.id), PARSE_MODE_NONE),
            ("MESSAGE_ID", str(self.message.reference.message_id), PARSE_MODE_NONE),
        ], bot=self.bot)

    async def build_interaction(self):
        if hasattr(self.message, 'interaction_metadata'):
            if not self.message.interaction_metadata:
                self.interaction = ""
                return
            command = "a slash command"
            user = self.message.interaction_metadata.user
            interaction_id = self.message.interaction_metadata.id
        elif self.message.interaction:
            command = f"/{self.message.interaction.name}"
            user = self.message.interaction.user
            interaction_id = self.message.interaction.id
        else:
            self.interaction = ""
            return

        is_bot = _gather_user_bot(user)
        user_colour = await self._gather_user_colour(user)
        avatar_url = user.display_avatar if user.display_avatar else DiscordUtils.default_avatar

        self.interaction = await fill_out(self.guild, message_interaction, [
            ("AVATAR_URL", str(avatar_url), PARSE_MODE_NONE),
            ("BOT_TAG", is_bot, PARSE_MODE_NONE),
            ("NAME_TAG", await discriminator(user.name, user.discriminator), PARSE_MODE_NONE),
            ("NAME", str(html.escape(user.display_name))),
            ("COMMAND", re.sub(r'\/([a-zA-Z]+)\d+', r'/\1', str(command)), PARSE_MODE_NONE),
            ("USER_COLOUR", user_colour, PARSE_MODE_NONE),
            ("FILLER", "used ", PARSE_MODE_NONE),
            ("USER_ID", str(user.id), PARSE_MODE_NONE),
            ("INTERACTION_ID", str(interaction_id), PARSE_MODE_NONE),
        ], bot=self.bot)

    async def build_sticker(self):
        sticker = None
        sticker_image_url = None
        
        if self.message.stickers and hasattr(self.message.stickers[0], "url"):
            sticker_image_url = self.message.stickers[0].url
        if not sticker_image_url:
            for snapshot in self.get_message_snapshots():
                if hasattr(snapshot, "stickers") and snapshot.stickers and hasattr(snapshot.stickers[0], "url"):
                    sticker_image_url = snapshot.stickers[0].url
                    self.message.reference = message_reference_forwarded
                    break

        if not sticker_image_url:
            return
            

        if sticker_image_url.endswith(".json"):
            try:
                sticker = await self.message.stickers[0].fetch()
            except:
                for snapshot in self.get_message_snapshots():
                    if hasattr(snapshot, "stickers") and snapshot.stickers and hasattr(snapshot.stickers[0], "url"):
                        sticker = await snapshot.stickers[0].fetch()
                        break
            sticker_image_url = (
                f"https://cdn.jsdelivr.net/gh/mahtoid/DiscordUtils@master/stickers/{sticker.pack_id}/{sticker.id}.gif"
            )

        self.message.content = await fill_out(self.guild, img_attachment, [
            ("ATTACH_URL", str(sticker_image_url), PARSE_MODE_NONE),
            ("ATTACH_URL_THUMB", str(sticker_image_url), PARSE_MODE_NONE)
        ], bot=self.bot)

    async def _fetch_raw_message_data(self):
        if not self.bot or not hasattr(self.bot, "http") or not getattr(self.bot.http, "token", None):
            return None
        token = self.bot.http.token
        url = f"https://discord.com/api/v10/channels/{self.message.channel.id}/messages/{self.message.id}"
        headers = {"Authorization": f"{token}"}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=5) as resp:
                    if resp.status == 200:
                        return await resp.json()
        except Exception:
            pass
        return None

    async def build_assets(self):
        processed_attachments = []
        attachment_urls = set()

        for snapshot in self.get_message_snapshots():
            if hasattr(snapshot, "embeds"):
                for se in snapshot.embeds:
                    self.embeds += await Embed(se, self.guild).flow()
                    self.message.reference = await fill_out(self.guild, message_reference_forwarded, [
                        ("FORWARD_ICON", DiscordUtils.forward_icon, PARSE_MODE_NONE),
                    ])

        for a in self.message.attachments:
            if self.attachment_handler and isinstance(self.attachment_handler, AttachmentHandler):
                a = await self.attachment_handler.process_asset(a)
            processed_attachments.append(a)
            attachment_urls.update(self._collect_attachment_urls(a))

        for e in self.message.embeds:
            if self._is_duplicate_image_embed(e, attachment_urls):
                continue
            self.embeds += await Embed(e, self.guild, self.pytz_timezone, self.military_time).flow()

        for a in processed_attachments:
            self.attachments += await Attachment(a, self.guild).flow()
        
        for snapshot in self.get_message_snapshots():
            if hasattr(snapshot, "attachments"):
                for sa in snapshot.attachments:
                    if self.attachment_handler:
                        sa = await self.attachment_handler.process_asset(sa)
                    self.attachments += await Attachment(sa,self.guild).flow()
                    self.message.reference = message_reference_forwarded

        # DEEP FETCH FALLBACK for V2 Components missing from library object
        components = self.message.components
        if not components:
            raw_data = await self._fetch_raw_message_data()
            if raw_data and "components" in raw_data:
                components = raw_data["components"]

        if components:
            for c in components:
                self.components += await Component(c, self.guild, self.message.attachments).flow()

        for snapshot in self.get_message_snapshots():
            if hasattr(snapshot, "components"):
                for ac in snapshot.components:
                    self.components += await Component(ac,self.guild).flow()
                    self.message.reference = message_reference_forwarded

        for r in self.message.reactions:
            self.reactions += await Reaction(r, self.guild).flow()

        if self.reactions:
            self.reactions = f'<div class="chatlog__reactions">{self.reactions}</div>'

    async def build_message_template(self):
        started = await self.generate_message_divider()

        if started:
            return self.message_html

        self.message_html += await fill_out(self.guild, message_body, [
            ("MESSAGE_ID", str(self.message.id)),
            ("MESSAGE_CONTENT", self.message.content, PARSE_MODE_NONE),
            ("EMBEDS", self.embeds, PARSE_MODE_NONE),
            ("ATTACHMENTS", self.attachments, PARSE_MODE_NONE),
            ("COMPONENTS", self.components, PARSE_MODE_NONE),
            ("EMOJI", self.reactions, PARSE_MODE_NONE),
            ("TIMESTAMP", self.message_created_at, PARSE_MODE_NONE),
            ("TIME", self.message_created_at.split(maxsplit=4)[4], PARSE_MODE_NONE),
        ], bot=self.bot)

        return self.message_html

    def _generate_message_divider_check(self):
        return bool(
            self.previous_message is None or self.message.reference != "" or
            self.previous_message.type is not discord.MessageType.default or self.interaction != "" or
            self.previous_message.author.id != self.message.author.id or self.message.webhook_id is not None or
            self.message.created_at > (self.previous_message.created_at + timedelta(minutes=4))
        )

    async def generate_message_divider(self, channel_audit=False):
        if channel_audit or self._generate_message_divider_check():
            if self.previous_message is not None:
                self.message_html += await fill_out(self.guild, end_message, [], bot=self.bot)

            if channel_audit:
                self.audit = True
                return

            followup_symbol = ""
            is_bot = _gather_user_bot(self.message.author)
            avatar_url = self.message.author.display_avatar if self.message.author.display_avatar else DiscordUtils.default_avatar

            if self.message.reference != "" or self.interaction:
                followup_symbol = "<div class='chatlog__followup-symbol'></div>"

            time = self.message.created_at
            if not self.message.created_at.tzinfo:
                time = timezone("UTC").localize(time)

            if self.military_time:
                default_timestamp = time.astimezone(timezone(self.pytz_timezone)).strftime("%d-%m-%Y %H:%M")
            else:
                default_timestamp = time.astimezone(timezone(self.pytz_timezone)).strftime("%d-%m-%Y %I:%M %p")

            self.message_html += await fill_out(self.guild, start_message, [
                ("REFERENCE_SYMBOL", followup_symbol, PARSE_MODE_NONE),
                ("REFERENCE", self.message.reference if self.message.reference else self.interaction,
                 PARSE_MODE_NONE),
                ("AVATAR_URL", str(avatar_url), PARSE_MODE_NONE),
                ("NAME_TAG", await discriminator(self.message.author.name, self.message.author.discriminator), PARSE_MODE_NONE),
                ("USER_ID", str(self.message.author.id)),
                ("USER_COLOUR", await self._gather_user_colour(self.message.author)),
                ("USER_ICON", await self._gather_user_icon(self.message.author), PARSE_MODE_NONE),
                ("NAME", str(html.escape(self.message.author.display_name))),
                ("BOT_TAG", str(is_bot), PARSE_MODE_NONE),
                ("TIMESTAMP", str(self.message_created_at)),
                ("DEFAULT_TIMESTAMP", str(default_timestamp), PARSE_MODE_NONE),
                ("MESSAGE_ID", str(self.message.id)),
                ("MESSAGE_CONTENT", self.message.content, PARSE_MODE_NONE),
                ("EMBEDS", self.embeds, PARSE_MODE_NONE),
                ("ATTACHMENTS", self.attachments, PARSE_MODE_NONE),
                ("COMPONENTS", self.components, PARSE_MODE_NONE),
                ("EMOJI", self.reactions, PARSE_MODE_NONE)
            ], bot=self.bot)

            return True

    async def build_pin_template(self):
        self.message_html += await fill_out(self.guild, message_pin, [
            ("PIN_URL", DiscordUtils.pinned_message_icon, PARSE_MODE_NONE),
            ("USER_COLOUR", await self._gather_user_colour(self.message.author)),
            ("NAME", str(html.escape(self.message.author.display_name))),
            ("NAME_TAG", await discriminator(self.message.author.name, self.message.author.discriminator), PARSE_MODE_NONE),
            ("MESSAGE_ID", str(self.message.id), PARSE_MODE_NONE),
            ("REF_MESSAGE_ID", str(self.message.reference.message_id) if self.message.reference else "", PARSE_MODE_NONE)
        ], bot=self.bot)

    async def build_thread_template(self):
        self.message_html += await fill_out(self.guild, message_thread, [
            ("THREAD_URL", DiscordUtils.thread_channel_icon,
             PARSE_MODE_NONE),
            ("THREAD_NAME", self.message.content, PARSE_MODE_NONE),
            ("USER_COLOUR", await self._gather_user_colour(self.message.author)),
            ("NAME", str(html.escape(self.message.author.display_name))),
            ("NAME_TAG", await discriminator(self.message.author.name, self.message.author.discriminator), PARSE_MODE_NONE),
            ("MESSAGE_ID", str(self.message.id), PARSE_MODE_NONE),
        ], bot=self.bot)

    async def build_remove(self):
        removed_member: discord.Member = self.message.mentions[0]
        self.message_html += await fill_out(self.guild, message_thread_remove, [
            ("THREAD_URL", DiscordUtils.thread_remove_recipient,
             PARSE_MODE_NONE),
            ("USER_COLOUR", await self._gather_user_colour(self.message.author)),
            ("NAME", str(html.escape(self.message.author.display_name))),
            ("NAME_TAG", await discriminator(self.message.author.name, self.message.author.discriminator),
             PARSE_MODE_NONE),
            ("RECIPIENT_USER_COLOUR", await self._gather_user_colour(removed_member)),
            ("RECIPIENT_NAME", str(html.escape(removed_member.display_name))),
            ("RECIPIENT_NAME_TAG", await discriminator(removed_member.name, removed_member.discriminator),
             PARSE_MODE_NONE),
            ("MESSAGE_ID", str(self.message.id), PARSE_MODE_NONE),
        ], bot=self.bot)

    async def build_add(self):
        removed_member: discord.Member = self.message.mentions[0]
        self.message_html += await fill_out(self.guild, message_thread_add, [
            ("THREAD_URL", DiscordUtils.thread_add_recipient,
             PARSE_MODE_NONE),
            ("USER_COLOUR", await self._gather_user_colour(self.message.author)),
            ("NAME", str(html.escape(self.message.author.display_name))),
            ("NAME_TAG", await discriminator(self.message.author.name, self.message.author.discriminator),
             PARSE_MODE_NONE),
            ("RECIPIENT_USER_COLOUR", await self._gather_user_colour(removed_member)),
            ("RECIPIENT_NAME", str(html.escape(removed_member.display_name))),
            ("RECIPIENT_NAME_TAG", await discriminator(removed_member.name, removed_member.discriminator),
             PARSE_MODE_NONE),
            ("MESSAGE_ID", str(self.message.id), PARSE_MODE_NONE),
        ], bot=self.bot)

    @cache()
    async def _gather_member(self, author: Union[discord.Member, discord.User]):
        if not self.guild:
            return author

        member = self.guild.get_member(author.id)

        if member:
            return member

        try:
            return await self.guild.fetch_member(author.id)
        except Exception:
            return author

    async def _gather_user_colour(self, author: Union[discord.Member, discord.User]):
        member = await self._gather_member(author)
        
        # In DMs, member might be a User object which doesn't have .colour (use .accent_color or default)
        user_colour = "#FFFFFF"
        if member:
            if hasattr(member, "colour") and str(member.colour) != "#000000":
                user_colour = member.colour
            elif hasattr(member, "accent_color") and member.accent_color:
                user_colour = member.accent_color
        
        return f"color: {user_colour};"

    async def _gather_user_icon(self, author: discord.Member):
        member = await self._gather_member(author)

        if not member:
            return ""

        if hasattr(member, "display_icon") and member.display_icon:
            return f"<img class='chatlog__role-icon' src='{member.display_icon}' alt='Role Icon'>"
        elif hasattr(member, "top_role") and member.top_role and member.top_role.icon:
            return f"<img class='chatlog__role-icon' src='{member.top_role.icon}' alt='Role Icon'>"
        return ""

    def set_time(self, message: Optional[discord.Message] = None):
        message = message if message else self.message
        created_at_str = self.to_local_time_str(message.created_at)
        edited_at_str = self.to_local_time_str(message.edited_at) if message.edited_at else ""

        return created_at_str, edited_at_str

    def to_local_time_str(self, time):
        if not self.message.created_at.tzinfo:
            time = timezone("UTC").localize(time)

        local_time = time.astimezone(timezone(self.pytz_timezone))

        return local_time.strftime(self.time_format)


async def gather_messages(
    messages: List[discord.Message],
    guild: Optional[discord.Guild],
    pytz_timezone,
    military_time,
    attachment_handler: Optional[AttachmentHandler],
    bot: Optional[discord.Client] = None,
) -> (str, dict):
    message_html: str = ""
    meta_data: dict = {}
    previous_message: Optional[discord.Message] = None

    message_dict = {message.id: message for message in messages}

    if messages and "thread" in str(messages[0].channel.type) and messages[0].reference and guild:
        channel = guild.get_channel(messages[0].reference.channel_id)

        if not channel:
            channel = await guild.fetch_channel(messages[0].reference.channel_id)

        message = await channel.fetch_message(messages[0].reference.message_id)
        messages[0] = message
        messages[0].reference = None

    for message in messages:
        content_html, meta_data = await MessageConstruct(
            message,
            previous_message,
            pytz_timezone,
            military_time,
            guild,
            meta_data,
            message_dict,
            attachment_handler,
            bot=bot
            ).construct_message()

        message_html += content_html
        previous_message = message

    message_html += "</div>"
    return message_html, meta_data