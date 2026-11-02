"""Build the website from the pipeline output.
Usage:  python build_site.py data.json [analysis_data.json]
  -> writes index.html (replay) and, if analysis_data.json is given, analysis.html (analysis dashboard)"""
import sys
from pathlib import Path
here = Path(__file__).resolve().parent

def build(tpl, data_path, out):
    data = Path(data_path).read_text(encoding="utf-8")
    html = (here / tpl).read_text(encoding="utf-8").replace("__DATA__", data.replace("</", "<\\/"))
    (here / out).write_text(html, encoding="utf-8")
    print(f"{out} written ({len(html)//1024} KB)")

build("tpl.html", sys.argv[1] if len(sys.argv) > 1 else here / "data.json", "index.html")
if len(sys.argv) > 2:
    build("analysis_tpl.html", sys.argv[2], "analysis.html")
