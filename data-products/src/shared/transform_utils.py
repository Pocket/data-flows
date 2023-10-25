import re

import html2text


class HtmlToText:
    def __init__(self):
        h = html2text.HTML2Text()
        h.ignore_links = True
        h.ignore_images = True
        h.ignore_emphasis = True
        h.body_width = 1024
        h.strong_mark = ""
        h.emphasis_mark = ""
        h.ul_item_mark = ""

        re_dummy = re.compile("(.^)(.^)")
        html2text.config.RE_MD_BACKSLASH_MATCHER = re_dummy
        html2text.config.RE_MD_DOT_MATCHER = re_dummy
        html2text.config.RE_MD_PLUS_MATCHER = re_dummy
        html2text.config.RE_MD_DASH_MATCHER = re_dummy

        self.h = h

    def get_text(self, html):
        text = self.h.handle(str(html))

        # Remove heading markdown
        text = re.sub(r"#+\s+", "", text)
        # Remove table markdown (ignore_tables doesn't add spacing inbetween columns)
        text = re.sub(r"-+\|[\-\|]+ *\n", "", text)
        text = re.sub(r"\| ", "\n", text)
        # Remove horizontal rules
        text = re.sub(r"\* \* \*\n\n", "", text)
        # Remove trailing whitespace
        text = re.sub(r"\s*?(\n{1,2})\s*", r"\1", text)
        text = text.strip()

        return text


def get_text_from_html(html):
    return HtmlToText().get_text(html)
