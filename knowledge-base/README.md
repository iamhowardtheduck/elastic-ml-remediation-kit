# Optional knowledge base: a book on designing ML jobs

The `ml-job-design-guide` skill gives the agent a search tool (`ml_book_search`) over a reference
text, so its configuration recommendations (bucket_span, detectors, split fields, `query_delay`,
memory sizing) cite chapter and page. We used *Machine Learning with the Elastic Stack*
(Collier & Azarmi, Packt 2019). **No book content ships with this kit.** Index your own licensed
copy, or any manual with page markers.

## Build it

1. Upload the PDF with Kibana's file upload, or export it to text. The upload creates one document with the text in `attachment.content`.
2. Have a `text_embedding` inference endpoint ready, for example `.jina-embeddings-v5-text-small` or `.multilingual-e5-small-elasticsearch`.
3. Check the split, then index:
   ```bash
   python3 knowledge-base/build_passages.py --source-index <uploaded-index> \
       --inference-id <endpoint> --title "<Book title>" --dry-run
   python3 knowledge-base/build_passages.py --source-index <uploaded-index> \
       --inference-id <endpoint> --title "<Book title>"
   python3 install.py --book yes
   ```
   The defaults match Packt-style footers (`<Chapter title> Chapter <n>` then `[ <page> ]`). For other layouts, use `--page-regex`, `--chapter-regex` and `--stop-regex`.

## How retrieval works

- Each page becomes one passage (about 220 words), carrying 40 words of the previous page. Footers and repeated lines are removed, and oversized pages are split at paragraph breaks.
- `content_semantic` is a `semantic_text` field on your endpoint; `content` is the same text for keyword matching.
- `ml_book_search` runs `FORK` (semantic, keyword) `| FUSE` and returns 5 pages with chapter and page numbers, about 1–2k tokens.

If you use a different book, edit the tool description in `ml-remediation/tools/ml_book_search.json` and the skill text in `ml-remediation/skills/ml-job-design-guide.json`.
