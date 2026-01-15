# src/utils/html.py
import re
from bs4 import BeautifulSoup

from .normalize import normalize_space

def strip_html_keep_lines(html: str) -> str:
    html = re.sub(r"</(p|tr|th|td|div|section|title|li|ul|ol)\s*>", "\n", html, flags=re.I)
    html = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
    html = re.sub(r"<[^>]+>", "", html)
    html = re.sub(r"[ \t]+\n", "\n", html)
    html = re.sub(r"\n{3,}", "\n\n", html)
    return html.strip()

def remove_tables_html(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for t in soup.find_all("table"):
        t.decompose()
    return str(soup)

def get_label_preserve_indent(tag) -> str:
    s = tag.get_text("", strip=False)
    s = s.replace("\r", "").replace("\t", "")
    s = re.sub(r"[ \u00A0]+$", "", s)
    s = re.sub(r"\n+", " ", s)
    return s

def normalize_label_clean(s: str, NBSP: str, FULLWIDTH_SPACE: str) -> str:
    if not s:
        return ""
    s = s.lstrip(" \t\r\n" + NBSP)
    s = s.lstrip(FULLWIDTH_SPACE)
    return normalize_space(s)

def count_indent(label: str, NBSP: str, FULLWIDTH_SPACE: str) -> int:
    if not label:
        return 0
    s = label.lstrip(" \t\r\n" + NBSP)
    i = 0
    while i < len(s) and s[i] == FULLWIDTH_SPACE:
        i += 1
    return i
