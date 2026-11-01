"""Build the website from the pipeline output.
Usage:  python build_site.py data.json      ->  writes index.html (next to tpl.html)"""
import sys
from pathlib import Path
here = Path(__file__).resolve().parent
data = Path(sys.argv[1] if len(sys.argv) > 1 else here / "data.json").read_text(encoding="utf-8")
html = (here / "tpl.html").read_text(encoding="utf-8").replace("__DATA__", data.replace("</", "<\\/"))
(here / "index.html").write_text(html, encoding="utf-8")
print(f"index.html written ({len(html)//1024} KB)")
