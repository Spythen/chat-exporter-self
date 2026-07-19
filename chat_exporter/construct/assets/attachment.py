import math
import os
import html

from chat_exporter.ext.discord_utils import DiscordUtils
from chat_exporter.ext.html_generator import (
    fill_out,
    img_attachment,
    msg_attachment,
    audio_attachment,
    video_attachment,
    PARSE_MODE_NONE,
)


class Attachment:
    def __init__(self, attachments, guild):
        self.attachments = attachments
        self.guild = guild

    async def flow(self):
        await self.build_attachment()
        return self.attachments

    async def build_attachment(self):
        is_spoiler = self._is_spoiler()

        if self.attachments.content_type is not None:
            if "image" in self.attachments.content_type:
                await self.image()
                if is_spoiler:
                    self._mark_spoiler()
                return
            elif "video" in self.attachments.content_type:
                await self.video()
                if is_spoiler:
                    self._mark_spoiler()
                return
            elif "audio" in self.attachments.content_type:
                await self.audio()
                if is_spoiler:
                    self._mark_spoiler()
                return

        await self.file()
        if is_spoiler:
            self._mark_spoiler()

    async def image(self):
        self.attachments = await fill_out(self.guild, img_attachment, [
            ("ATTACH_URL", self.attachments.proxy_url, PARSE_MODE_NONE),
            ("ATTACH_URL_THUMB", self.attachments.proxy_url, PARSE_MODE_NONE)
        ])

    async def video(self):
        self.attachments = await fill_out(self.guild, video_attachment, [
            ("ATTACH_URL", self.attachments.proxy_url, PARSE_MODE_NONE)
        ])

    async def audio(self):
        file_icon = DiscordUtils.file_attachment_audio
        file_size = self.get_file_size(self.attachments.size)

        self.attachments = await fill_out(self.guild, audio_attachment, [
            ("ATTACH_ICON", file_icon, PARSE_MODE_NONE),
            ("ATTACH_URL", self.attachments.proxy_url, PARSE_MODE_NONE),
            ("ATTACH_BYTES", str(file_size), PARSE_MODE_NONE),
            ("ATTACH_AUDIO", self.attachments.proxy_url, PARSE_MODE_NONE),
            ("ATTACH_FILE", str(self.attachments.filename), PARSE_MODE_NONE)
        ])

    async def get_text_preview(self) -> str:
        filename = str(getattr(self.attachments, "filename", "") or "")
        ext = os.path.splitext(filename)[1].lower().strip('.')
        
        previewable_exts = {
            "txt", "py", "json", "js", "ts", "html", "css", "md", "ini", "conf",
            "log", "yaml", "yml", "xml", "c", "cpp", "h", "cs", "java", "sh", "bat", "ps1", "sql"
        }
        
        if ext not in previewable_exts:
            content_type = str(getattr(self.attachments, "content_type", "") or "")
            if not ("text" in content_type or "json" in content_type or "javascript" in content_type):
                return ""
                
        try:
            # Skip massive files to prevent slow rendering
            if self.attachments.size > 5 * 1024 * 1024:
                return ""
                
            content_bytes = await self.attachments.read()
            try:
                content_str = content_bytes.decode('utf-8')
            except UnicodeDecodeError:
                content_str = content_bytes.decode('latin-1', errors='replace')
                
            lines = content_str.splitlines()
            max_preview_lines = 5
            preview_lines = lines[:max_preview_lines]
            preview_text = "\n".join(preview_lines)
            
            escaped_text = html.escape(preview_text)
            
            remaining_bytes = self.attachments.size - len(preview_text.encode('utf-8'))
            if len(lines) > max_preview_lines or remaining_bytes > 0:
                if remaining_bytes > 0:
                    left_size = self.get_file_size(remaining_bytes)
                    left_text = f"... ({left_size} left)"
                else:
                    left_text = "... (remaining lines truncated)"
                left_html = f'<div class="chatlog__text-preview-left">{left_text}</div>'
            else:
                left_html = ""
                
            return f'<div class="chatlog__text-preview-container"><pre class="chatlog__text-preview-pre"><code>{escaped_text}</code></pre>{left_html}</div>'
        except Exception as e:
            print(f"Error generating text preview: {e}")
            return ""

    async def file(self):
        file_icon = await self.get_file_icon()
        file_size = self.get_file_size(self.attachments.size)
        text_preview = await self.get_text_preview()

        self.attachments = await fill_out(self.guild, msg_attachment, [
            ("ATTACH_ICON", file_icon, PARSE_MODE_NONE),
            ("ATTACH_URL", self.attachments.proxy_url, PARSE_MODE_NONE),
            ("ATTACH_BYTES", str(file_size), PARSE_MODE_NONE),
            ("ATTACH_FILE", str(self.attachments.filename), PARSE_MODE_NONE),
            ("TEXT_PREVIEW", text_preview, PARSE_MODE_NONE)
        ])

    @staticmethod
    def get_file_size(file_size):
        if file_size == 0:
            return "0 bytes"
        size_name = ("bytes", "KB", "MB")
        i = int(math.floor(math.log(file_size, 1024)))
        p = math.pow(1024, i)
        s = round(file_size / p, 2)
        return "%s %s" % (s, size_name[i])

    async def get_file_icon(self) -> str:
        return self.resolve_file_icon(
            name=str(getattr(self.attachments, "filename", "") or ""),
            content_type=str(getattr(self.attachments, "content_type", "") or ""),
            url=str(getattr(self.attachments, "proxy_url", "") or "")
        )

    @staticmethod
    def resolve_file_icon(name: str = "", content_type: str = "", url: str = "") -> str:
        acrobat_types = "pdf"
        webcode_types = "html", "htm", "css", "rss", "xhtml", "xml"
        code_types = "py", "cgi", "pl", "gadget", "jar", "msi", "wsf", "bat", "php", "js"
        document_types = (
            "txt", "doc", "docx", "rtf", "xls", "xlsx", "ppt", "pptx", "odt", "odp", "ods", "odg", "odf", "swx",
            "sxi", "sxc", "sxd", "stw"
        )
        archive_types = (
            "br", "rpm", "dcm", "epub", "zip", "tar", "rar", "gz", "bz2", "7x", "7z", "deb", "ar", "z", "lzo", "lz",
            "lz4", "arj", "pkg"
        )

        content_type = (content_type or "").lower()
        if content_type.startswith("audio/"):
            return DiscordUtils.file_attachment_audio

        def _extension_from(value: str) -> str:
            if not value:
                return ""
            cleaned = str(value).split("?", 1)[0].split("#", 1)[0]
            if "." not in cleaned:
                return ""
            return cleaned.rsplit(".", 1)[-1].lower()

        extension = ""
        for candidate in (name, url):
            extension = _extension_from(candidate)
            if extension:
                break

        if not extension and content_type:
            if "html" in content_type:
                extension = "html"
            elif "pdf" in content_type:
                extension = "pdf"

        if extension in acrobat_types:
            return DiscordUtils.file_attachment_acrobat
        elif extension in webcode_types:
            return DiscordUtils.file_attachment_webcode
        elif extension in code_types:
            return DiscordUtils.file_attachment_code
        elif extension in document_types:
            return DiscordUtils.file_attachment_document
        elif extension in archive_types:
            return DiscordUtils.file_attachment_archive

        return DiscordUtils.file_attachment_unknown

    def _is_spoiler(self) -> bool:
        """Check if an attachment is marked as a spoiler."""
        attachment = self.attachments
        spoiler_attr = getattr(attachment, "spoiler", None)
        if callable(spoiler_attr):
            try:
                return bool(spoiler_attr())
            except Exception:
                pass
        if spoiler_attr is not None:
            return bool(spoiler_attr)

        is_spoiler_method = getattr(attachment, "is_spoiler", None)
        if callable(is_spoiler_method):
            try:
                return bool(is_spoiler_method())
            except Exception:
                return False

        return False

    def _mark_spoiler(self):
        """Add spoiler styling class to the rendered attachment HTML."""
        if not isinstance(self.attachments, str):
            return

        replacements = (
            ('<div class=chatlog__attachment>', '<div class="chatlog__attachment chatlog__attachment-spoiler">'),
            ('class="chatlog__attachment"', 'class="chatlog__attachment chatlog__attachment-spoiler"'),
        )

        for target, replacement in replacements:
            if target in self.attachments:
                self.attachments = self.attachments.replace(target, replacement, 1)
                break
