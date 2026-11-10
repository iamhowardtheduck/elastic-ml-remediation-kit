#!/usr/bin/env python3
"""
Build the ml-book-passages knowledge-base index from YOUR OWN copy of a book or manual.
No book content ships with this kit.

Why: uploading a whole PDF gives a single huge document; an agent search tool would pull
the entire book into context on every lookup. This splits the text into one passage per page
(with chapter/page metadata for citations), carries ~40 words from the previous page for
continuity, drops footers and repeated lines, and indexes each passage with a semantic_text
field (your inference endpoint) plus a plain-text copy for hybrid search.

Source (one of):
  --source-index NAME   an index holding the extracted text, e.g. created by Kibana's
                        file upload of a PDF (field --text-field, default attachment.content)
  --text-file PATH      a plain-text export of the book

Examples:
  python3 build_passages.py --source-index my-uploaded-pdf --inference-id .jina-embeddings-v5-text-small \
      --title "Machine Learning with the Elastic Stack (Packt 2019)"
  python3 build_passages.py --text-file book.txt --inference-id .multilingual-e5-small-elasticsearch --dry-run

Page / chapter detection is regex based; the defaults match Packt-style footers:
      "<Chapter title> Chapter <n>"  followed by  "[ <page> ]"
Use --dry-run to check the split before indexing, and adjust --page-regex / --chapter-regex.
Uses the same ES_URL / ES_API_KEY (or ES_USERNAME+ES_PASSWORD) / ES_CA_CERT settings as install.py.
"""
import argparse, json, re, sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from install import Client, load_env  # noqa: E402

DEFAULT_PAGE = r"\n\[ (\d+) \]\n"
DEFAULT_CHAPTER = r"\n([A-Z][^\n]{3,80}) Chapter (\d+)\s*$"


def get_text(c, a):
    if a.text_file:
        return Path(a.text_file).read_text()
    st, r = c.es("POST", f"/{a.source_index}/_search", {"size": 1, "_source": [a.text_field]})
    if st != 200 or not r["hits"]["hits"]:
        sys.exit(f"cannot read {a.source_index}: {st} {json.dumps(r)[:300]}")
    src = r["hits"]["hits"][0]["_source"]
    for part in a.text_field.split("."):
        src = src[part]
    return src


def split(text, a):
    parts = re.split(a.page_regex, text)
    docs, chapter, title, prev_tail = [], 0, "Front matter", ""
    for i in range(1, len(parts), 2):
        page, body = int(parts[i]), parts[i - 1]
        if a.stop_regex and re.search(a.stop_regex, body):   # back matter (ads, index): stop here
            break
        m = re.search(a.chapter_regex, body)
        if m:
            title, chapter, body = m.group(1).strip(), int(m.group(2)), body[:m.start()]
        lines = []
        for ln in body.split("\n"):
            ln = ln.rstrip()
            if lines and ln and ln == lines[-1]:
                continue
            lines.append(ln)
        txt = re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).replace("​", "").strip()
        if len(txt.split()) < 25:
            prev_tail = " ".join(txt.split()[-40:])
            continue
        content = (f"...{prev_tail}\n\n" if prev_tail else "") + txt
        prev_tail = " ".join(txt.split()[-40:])
        base = {"book": a.title, "chapter": chapter, "chapter_title": title, "page": page}
        if len(content.split()) <= a.max_words:
            docs.append({**base, "content": content})
            continue
        cur, n, part = [], 0, 1                      # split oversized pages at paragraph breaks
        for p in content.split("\n\n"):
            cur.append(p); n += len(p.split())
            if n >= a.max_words * 2 // 3:
                docs.append({**base, "part": part, "content": "\n\n".join(cur)}); part += 1; cur, n = [], 0
        if cur:
            docs.append({**base, "part": part, "content": "\n\n".join(cur)})
    return docs


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--source-index")
    src.add_argument("--text-file")
    ap.add_argument("--text-field", default="attachment.content")
    ap.add_argument("--dest-index", default="ml-book-passages")
    ap.add_argument("--inference-id", required=True, help="text_embedding inference endpoint id")
    ap.add_argument("--title", default="Reference book")
    ap.add_argument("--page-regex", default=DEFAULT_PAGE)
    ap.add_argument("--chapter-regex", default=DEFAULT_CHAPTER)
    ap.add_argument("--stop-regex", default=r"\n(Other Books You May Enjoy|Index)\s*$",
                    help="stop at the first page whose footer matches (back matter); '' to disable")
    ap.add_argument("--max-words", type=int, default=600)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    c = Client(load_env())
    docs = split(get_text(c, a), a)
    if not docs:
        sys.exit("No pages detected - adjust --page-regex.")
    words = [len(d["content"].split()) for d in docs]
    print(f"{len(docs)} passages, words min/avg/max {min(words)}/{sum(words)//len(words)}/{max(words)}")
    for (ch, t), n in sorted(Counter((d["chapter"], d["chapter_title"]) for d in docs).items()):
        print(f"  ch {ch:>2}  {t[:60]:<60} {n:>4} passages")
    if a.dry_run:
        print("\n--dry-run: sample passage\n" + docs[min(10, len(docs) - 1)]["content"][:600])
        return

    mapping = json.loads((Path(__file__).parent / "index/ml-book-passages.index.json").read_text()
                         .replace("@@inference_id@@", a.inference_id))
    st, _ = c.es("HEAD", f"/{a.dest_index}")
    if st == 200:
        sys.exit(f"{a.dest_index} already exists - delete it first to rebuild.")
    st, r = c.es("PUT", f"/{a.dest_index}", mapping)
    if st != 200:
        sys.exit(f"create index failed: {json.dumps(r)[:400]}")
    ok = err = 0
    for i in range(0, len(docs), 25):
        lines = []
        for d in docs[i:i + 25]:
            lines.append(json.dumps({"index": {"_index": a.dest_index, "_id": f"p{d['page']}-{d.get('part', 1)}"}}))
            lines.append(json.dumps(d))
        _, res = bulk(c, "\n".join(lines) + "\n")
        for it in res.get("items", []):
            if it["index"].get("error"):
                err += 1
            else:
                ok += 1
    c.es("POST", f"/{a.dest_index}/_refresh")
    print(f"indexed {ok}, errors {err} -> {a.dest_index}. Now run: python3 install.py --book yes")


def bulk(c, ndjson):
    import urllib.request
    req = urllib.request.Request(c.es_base + "/_bulk", data=ndjson.encode(), method="POST",
                                 headers={"Authorization": c.auth, "Content-Type": "application/x-ndjson"})
    with urllib.request.urlopen(req, context=c.ctx, timeout=600) as r:
        return r.status, json.loads(r.read())


if __name__ == "__main__":
    main()
