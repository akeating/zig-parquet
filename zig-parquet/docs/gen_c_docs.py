#!/usr/bin/env python3

from __future__ import annotations

import html
import re
import shutil
import sys
from pathlib import Path


DOC_START = "/**"
DOC_END = "*/"


def clean_doc_block(block: list[str]) -> list[str]:
    lines: list[str] = []
    for raw in block:
        line = raw.rstrip("\n")
        line = line.strip()
        if line.startswith(DOC_START):
            line = line[len(DOC_START) :].strip()
        if line.endswith(DOC_END):
            line = line[: -len(DOC_END)].strip()
        if line.startswith("*"):
            line = line[1:].lstrip()
        lines.append(line)
    while lines and lines[0] == "":
        lines.pop(0)
    while lines and lines[-1] == "":
        lines.pop()
    return lines


def render_inline(text: str) -> str:
    escaped = html.escape(text)
    return re.sub(r"`([^`]+)`", r"<code>\1</code>", escaped)


def render_doc_lines(lines: list[str]) -> str:
    if not lines:
        return ""

    rendered: list[str] = []
    paragraph: list[str] = []
    in_list = False
    current_list_item: list[str] | None = None

    def flush_paragraph() -> None:
        nonlocal paragraph
        if paragraph:
            rendered.append(f"<p>{render_inline(' '.join(paragraph))}</p>")
            paragraph = []

    def flush_list_item() -> None:
        nonlocal current_list_item
        if current_list_item is not None:
            rendered.append(f"<li>{render_inline(' '.join(current_list_item))}</li>")
            current_list_item = None

    def close_list() -> None:
        nonlocal in_list
        if in_list:
            flush_list_item()
            rendered.append("</ul>")
            in_list = False

    for line in lines:
        if not line:
            flush_paragraph()
            close_list()
            continue
        if line.startswith("- "):
            flush_paragraph()
            if not in_list:
                rendered.append("<ul>")
                in_list = True
            flush_list_item()
            current_list_item = [line[2:]]
            continue
        if in_list and current_list_item is not None:
            current_list_item.append(line)
            continue
        close_list()
        paragraph.append(line)

    flush_paragraph()
    close_list()
    return "\n".join(rendered)


def declaration_name(decl: str) -> str:
    normalized = " ".join(decl.split())

    match = re.search(r"}\s*([A-Za-z_][A-Za-z0-9_]*)\s*;$", normalized)
    if match:
        return match.group(1)

    match = re.search(r"typedef\s+struct\s+[A-Za-z_][A-Za-z0-9_]*\s+([A-Za-z_][A-Za-z0-9_]*)\s*;$", normalized)
    if match:
        return match.group(1)

    match = re.search(r"typedef\s+enum\s+[A-Za-z_][A-Za-z0-9_]*\s+([A-Za-z_][A-Za-z0-9_]*)\s*;$", normalized)
    if match:
        return match.group(1)

    match = re.search(r"([A-Za-z_][A-Za-z0-9_]*)\s*\([^;]*\)\s*;$", normalized)
    if match:
        return match.group(1)

    return "declaration"


def declaration_kind(decl: str) -> str:
    normalized = " ".join(decl.split())
    if normalized.startswith("typedef enum"):
        return "enum"
    if normalized.startswith("typedef struct"):
        return "struct"
    if "(" in normalized and normalized.endswith(";"):
        return "function"
    return "declaration"


def parse_header(text: str) -> list[dict[str, str | list[str]]]:
    lines = text.splitlines(keepends=True)
    i = 0
    items: list[dict[str, str | list[str]]] = []
    pending_doc: list[str] | None = None

    while i < len(lines):
        stripped = lines[i].strip()

        if stripped.startswith(DOC_START):
            block: list[str] = []
            while i < len(lines):
                block.append(lines[i])
                if DOC_END in lines[i]:
                    break
                i += 1
            pending_doc = clean_doc_block(block)
            i += 1
            continue

        if not stripped or stripped.startswith("#") or stripped == 'extern "C" {' or stripped == "}":
            if pending_doc and not stripped:
                items.append({"kind": "section", "doc": pending_doc})
                pending_doc = None
            i += 1
            continue

        if stripped.startswith("//"):
            i += 1
            continue

        start = i
        brace_depth = 0
        while i < len(lines):
            current = lines[i]
            brace_depth += current.count("{")
            brace_depth -= current.count("}")
            if current.strip().endswith(";") and brace_depth <= 0:
                break
            i += 1
        decl_lines = lines[start : i + 1]
        decl = "".join(decl_lines).strip()

        item = {
            "kind": declaration_kind(decl),
            "name": declaration_name(decl),
            "doc": pending_doc or [],
            "decl": decl,
        }
        items.append(item)
        pending_doc = None
        i += 1

    if pending_doc:
        items.append({"kind": "section", "doc": pending_doc})

    return items


def render_html(items: list[dict[str, str | list[str]]]) -> str:
    toc_types: list[str] = []
    toc_functions: list[str] = []
    sections: list[str] = []

    for item in items:
        kind = item["kind"]
        if kind == "section":
            sections.append(
                "<section class=\"section-note\">"
                f"{render_doc_lines(item['doc'])}"
                "</section>"
            )
            continue

        name = str(item["name"])
        doc = item["doc"]
        decl = str(item["decl"])
        anchor = name
        entry = f"<li><a href=\"#{anchor}\"><code>{html.escape(name)}</code></a></li>"
        if kind == "function":
            toc_functions.append(entry)
        else:
            toc_types.append(entry)

        sections.append(
            "<section class=\"symbol\">"
            f"<h2 id=\"{anchor}\"><code>{html.escape(name)}</code></h2>"
            f"<div class=\"kind\">{html.escape(str(kind))}</div>"
            f"{render_doc_lines(doc)}"
            f"<pre><code>{html.escape(decl)}</code></pre>"
            "</section>"
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>zig-parquet C API</title>
  <style>
    :root {{
      color-scheme: light dark;
      --bg: #ffffff;
      --fg: #1f2328;
      --muted: #59636e;
      --panel: #f6f8fa;
      --border: #d0d7de;
      --link: #0969da;
    }}
    @media (prefers-color-scheme: dark) {{
      :root {{
        --bg: #0d1117;
        --fg: #e6edf3;
        --muted: #8b949e;
        --panel: #161b22;
        --border: #30363d;
        --link: #58a6ff;
      }}
    }}
    body {{
      margin: 0;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--fg);
      line-height: 1.55;
    }}
    main {{
      max-width: 1080px;
      margin: 0 auto;
      padding: 2rem;
    }}
    h1, h2, h3 {{
      line-height: 1.2;
    }}
    a {{
      color: var(--link);
    }}
    .lead {{
      color: var(--muted);
      max-width: 70ch;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 1rem;
      margin: 2rem 0;
    }}
    .panel, .symbol, .section-note {{
      border: 1px solid var(--border);
      background: var(--panel);
      border-radius: 12px;
      padding: 1rem 1.25rem;
    }}
    .kind {{
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
      font-size: 0.8rem;
      margin-bottom: 0.75rem;
    }}
    pre {{
      overflow-x: auto;
      padding: 0.9rem;
      border-radius: 10px;
      border: 1px solid var(--border);
      background: var(--bg);
    }}
    code {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    }}
    ul {{
      padding-left: 1.25rem;
    }}
    .symbol {{
      margin-top: 1rem;
    }}
  </style>
</head>
<body>
  <main>
    <p><a href="../index.html">Back to docs landing page</a></p>
    <h1>C API Reference</h1>
    <p class="lead">
      Generated from <code>include/libparquet.h</code>, which is the public C ABI source of truth.
      Use this reference for function signatures, ownership rules, and cleanup expectations.
    </p>
    <div class="grid">
      <section class="panel">
        <h2>Types</h2>
        <ul>
          {''.join(toc_types)}
        </ul>
      </section>
      <section class="panel">
        <h2>Functions</h2>
        <ul>
          {''.join(toc_functions)}
        </ul>
      </section>
    </div>
    {''.join(sections)}
  </main>
</body>
</html>
"""


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: gen_c_docs.py <header> <output_dir>", file=sys.stderr)
        return 1

    header = Path(sys.argv[1])
    out_dir = Path(sys.argv[2])
    out_dir.mkdir(parents=True, exist_ok=True)

    text = header.read_text(encoding="utf-8")
    items = parse_header(text)
    (out_dir / "index.html").write_text(render_html(items), encoding="utf-8")
    shutil.copy2(header, out_dir / header.name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
