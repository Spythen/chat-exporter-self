import html
import re
from chat_exporter.ext.emoji_convert import convert_emoji


class ParseMarkdown:
    def __init__(self, content):
        self.content = str(content if content is not None else "")
        self.code_blocks_content = []


    async def standard_message_flow(self):
        return await self.standard_embed_flow()

    async def link_embed_flow(self):
        self.parse_embed_markdown()
        await self.parse_emoji()

    async def standard_embed_flow(self):
        self.parse_code_block_markdown()
        self.parse_embed_markdown() # Named links first
        self.https_http_links()    # Raw links second
        self.parse_normal_markdown()

        await self.parse_emoji()
        self.reverse_code_block_markdown()
        self.content = self.content.replace("\n", "<br>")
        return self.content

    async def special_embed_flow(self):
        self.https_http_links()
        self.parse_code_block_markdown()
        self.parse_normal_markdown()

        await self.parse_emoji()
        self.reverse_code_block_markdown()
        self.content = self.content.replace("\n", "<br>")
        return self.content

    async def message_reference_flow(self):
        self.strip_preserve()
        self.parse_code_block_markdown(reference=True)
        self.https_http_links()
        self.parse_normal_markdown()

        await self.parse_emoji()
        self.reverse_code_block_markdown()
        self.content = self.content.replace("\n", "<br>")
        return self.content

    async def special_emoji_flow(self):
        await self.parse_emoji()
        return self.content

    def strip_preserve(self):
        self.content = re.sub(r"\n", " ", self.content)

    async def parse_emoji(self):
        self.content = await convert_emoji(self.content)

    def parse_normal_markdown(self):
        holder = (
            [r"\*\*\*(.*?)\*\*\*", '<strong><em>%s</em></strong>'],
            [r"\*\*(.*?)\*\*", '<strong>%s</strong>'],
            [r"\*(.*?)\*", '<em>%s</em>'],
            [r"__(.*?)__", '<span style="text-decoration: underline">%s</span>'],
            [r"(?<!\w)_(.*?)_(?!\w)", '<em>%s</em>'],
            [r"~~(.*?)~~", '<span style="text-decoration: line-through">%s</span>'],
            [r"(?:^|\n|<br>)((?:>\s*|>>>\s*|&gt;\s*|&gt;&gt;&gt;\s*)*)###\s+(.*?)(?=\r?\n|<br>|$)", r"\1<h3>\2</h3>"],
            [r"(?:^|\n|<br>)((?:>\s*|>>>\s*|&gt;\s*|&gt;&gt;&gt;\s*)*)##\s+(.*?)(?=\r?\n|<br>|$)", r"\1<h2>\2</h2>"],
            [r"(?:^|\n|<br>)((?:>\s*|>>>\s*|&gt;\s*|&gt;&gt;&gt;\s*)*)#\s+(.*?)(?=\r?\n|<br>|$)", r"\1<h1>\2</h1>"],
            [r"^-#\s(.*?)\n", '<small>%s</small>'],
            [r"\|\|(.*?)\|\|", '<span class="spoiler spoiler--hidden" onclick="showSpoiler(event, this)"> <span '
                               'class="spoiler-text">%s</span></span>'],
        )

        for p, r in holder:
            if "\\" in r:  # Support for \1, \2, etc. (new header style)
                self.content = re.sub(p, r, self.content)
            else:  # Legacy %s style
                pattern = re.compile(p, re.M)
                match = re.search(pattern, self.content)
                while match is not None:
                    affected_text = match.group(1)
                    self.content = self.content.replace(self.content[match.start():match.end()], r % affected_text)
                    match = re.search(pattern, self.content)

        # > quote (group consecutive lines into a single block so the bar spans them)
        self.content = self.merge_quote_lines(self.content)

    def parse_code_block_markdown(self, reference=False):
        markdown_languages = ["asciidoc", "autohotkey", "bash", "coffeescript", "cpp", "cs", "css",
                              "diff", "fix", "glsl", "ini", "json", "md", "ml", "prolog", "py",
                              "tex", "xl", "xml", "js", "html"]
        self.content = re.sub(r"\n", "<br>", self.content)

        # ```code```
        pattern = re.compile(r"```(.*?)```")
        match = re.search(pattern, self.content)
        while match is not None:
            language_class = "nohighlight"
            affected_text = match.group(1)

            for language in markdown_languages:
                if affected_text.lower().startswith(language):
                    language_class = f"language-{language}"
                    _, _, affected_text = affected_text.partition('<br>')

            # Sanitizing command placeholders inside multiline code blocks
            affected_text = re.sub(r'(?i)(?P<prefix>\/|\?help\s+|s\.help\s+|[!&.?])(?P<name>[a-z_]+)\d+', r'\g<prefix>\g<name>', affected_text)
            affected_text = self.return_to_markdown(affected_text)

            second_pattern = re.compile(r"^<br>|<br>$")
            second_match = re.search(second_pattern, affected_text)
            while second_match is not None:
                affected_text = re.sub(r"^<br>|<br>$", '', affected_text)
                second_match = re.search(second_pattern, affected_text)
            affected_text = re.sub("  ", "&nbsp;&nbsp;", affected_text)

            self.code_blocks_content.append(affected_text)
            if not reference:
                self.content = self.content.replace(
                    self.content[match.start():match.end()],
                    '<div class="pre pre--multiline %s">%s</div>' % (language_class, f'%s{len(self.code_blocks_content)}')
                )
            else:
                self.content = self.content.replace(
                    self.content[match.start():match.end()],
                    '<span class="pre pre-inline">%s</span>' % f'%s{len(self.code_blocks_content)}'
                )

            match = re.search(pattern, self.content)

        # ``code``
        pattern = re.compile(r"``(.*?)``")
        match = re.search(pattern, self.content)
        while match is not None:
            affected_text = match.group(1)
            affected_text = self.return_to_markdown(affected_text)
            self.code_blocks_content.append(affected_text)
            self.content = self.content.replace(self.content[match.start():match.end()],
                                                '<span class="pre pre-inline">%s</span>' % f'%s{len(self.code_blocks_content)}')
            match = re.search(pattern, self.content)

        # `code`
        pattern = re.compile(r"`(.*?)`")
        match = re.search(pattern, self.content)
        while match is not None:
            affected_text = match.group(1)
            # Sanitizing command placeholders inside inline code blocks
            affected_text = re.sub(r'(?i)(?P<prefix>\/|\?help\s+|s\.help\s+|[!&.?])(?P<name>[a-z_]+)\d+', r'\g<prefix>\g<name>', affected_text)
            affected_text = self.return_to_markdown(affected_text)
            self.code_blocks_content.append(affected_text)
            self.content = self.content.replace(self.content[match.start():match.end()],
                                                '<span class="pre pre-inline">%s</span>' % f'%s{len(self.code_blocks_content)}')
            match = re.search(pattern, self.content)

        self.content = re.sub(r"<br>", "\n", self.content)

    def reverse_code_block_markdown(self):
        for x in range(len(self.code_blocks_content)):
            self.content = self.content.replace(f'%s{x + 1}', self.code_blocks_content[x])

    def parse_embed_markdown(self):
        # [Message](Link)
        # Improved regex to handle complex URLs and nested brackets
        pattern = re.compile(r"\[+([^\]]+)\]+\((https?://.*?)\)")
        match = re.search(pattern, self.content)
        while match is not None:
            affected_text = match.group(1)
            affected_url = match.group(2)
            # Ensure the URL is clean and doesn't leak into the output
            self.content = self.content.replace(
                self.content[match.start():match.end()],
                '<a href="%s" target="_blank">%s</a>' % (affected_url, affected_text)
            )
            match = re.search(pattern, self.content)

    def itercode_markdown(self):
        # This function seems to be used elsewhere for special parsing
        pass

    def return_to_markdown(self, content):
        content = content.replace("<strong><em>", "***").replace("</em></strong>", "***")
        content = content.replace("<strong>", "**").replace("</strong>", "**")
        content = content.replace("<em>", "*").replace("</em>", "*")
        content = content.replace('<span style="text-decoration: underline">', "__").replace("</span>", "__")
        return content

    def merge_quote_lines(self, content):
        lines = content.split("\n")
        merged_content = []
        quote_buffer = []
        multiline_quote = False

        for line in lines:
            if multiline_quote:
                quote_buffer.append(line)
                continue

            # Check for >>> (multiline quote)
            if line.startswith(">>> ") or line.startswith("&gt;&gt;&gt; "):
                multiline_quote = True
                prefix = ">>> " if line.startswith(">>> ") else "&gt;&gt;&gt; "
                quote_buffer.append(line[len(prefix):])
            elif line.startswith(">>>") or line.startswith("&gt;&gt;&gt;"):
                multiline_quote = True
                prefix = ">>>" if line.startswith(">>>") else "&gt;&gt;&gt;"
                quote_buffer.append(line[len(prefix):])
            # Check for > (single line quote)
            elif line.startswith("> ") or line.startswith("&gt; "):
                prefix = "> " if line.startswith("> ") else "&gt; "
                quote_buffer.append(line[len(prefix):])
            elif line.startswith(">") or line.startswith("&gt;"):
                prefix = ">" if line.startswith(">") else "&gt;"
                quote_buffer.append(line[len(prefix):])
            else:
                if quote_buffer:
                    quote_text = "\n".join(quote_buffer)
                    merged_content.append(f'<div class="quote">{quote_text}</div>')
                    quote_buffer = []
                merged_content.append(line)

        if quote_buffer:
            quote_text = "\n".join(quote_buffer)
            merged_content.append(f'<div class="quote">{quote_text}</div>')

        merged = "\n".join(merged_content)
        merged = re.sub(r"</div>[ \t]*\n(?!\n)", "</div>", merged)
        return merged

    def https_http_links(self):
        def remove_silent_link(url, raw_url=None):
            pattern = rf"`.*{raw_url}.*`"
            match = re.search(pattern, self.content)

            if "&lt;" in url and "&gt;" in url and not match:
                return url.replace("&lt;", "").replace("&gt;", "")
            return url

        content = re.sub("\n", "<br>", self.content)
        lines_output = []
        for line in content.split("<br>"):
            words_output = []
            for word in line.split(" "):
                if "http" not in word:
                    words_output.append(word)
                    continue

                if "&lt;" in word and "&gt;" in word:
                    pattern = r"&lt;https?:\/\/(.*)&gt;"
                    match_url = re.search(pattern, word)
                    if match_url:
                        match_url = match_url.group(1)
                        url = f'<a href="https://{match_url}">https://{match_url}</a>'
                        word = word.replace("https://" + match_url, url)
                        word = word.replace("http://" + match_url, url)
                    words_output.append(remove_silent_link(word, match_url))
                elif "https://" in word:
                    # Ignore URLs already inside an HTML tag or markdown link
                    if "href=" in word or 'src=' in word or "](" in word:
                        words_output.append(word)
                        continue

                    pattern = r"https://[^\s>`\"*]*"
                    word_link = re.search(pattern, word)
                    if word_link and word_link.group().endswith(")"):
                        words_output.append(word)
                        continue
                    elif word_link:
                        word_link = word_link.group()
                        word_full = f'<a href="{word_link}" target="_blank">{word_link}</a>'
                        word = re.sub(pattern, word_full, word)
                    words_output.append(remove_silent_link(word))
                elif "http://" in word:
                    # Ignore URLs already inside an HTML tag or markdown link
                    if "href=" in word or 'src=' in word or "](" in word:
                        words_output.append(word)
                        continue

                    pattern = r"http://[^\s>`\"*]*"
                    word_link = re.search(pattern, word)
                    if word_link and word_link.group().endswith(")"):
                        words_output.append(word)
                        continue
                    elif word_link:
                        word_link = word_link.group()
                        word_full = f'<a href="{word_link}" target="_blank">{word_link}</a>'
                        word = re.sub(pattern, word_full, word)
                    words_output.append(remove_silent_link(word))
                else:
                    words_output.append(word)
            lines_output.append(" ".join(words_output))
            
        self.content = "<br>".join(lines_output)
