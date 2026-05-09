from __future__ import annotations

from pathlib import Path

from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.shared import Pt


def main() -> None:
    md_path = Path("docs/INTERVIEW_PREP.md")
    out_path = Path("docs/INTERVIEW_PREP.docx")

    md = md_path.read_text(encoding="utf-8")
    lines = md.splitlines()

    doc = Document()
    style = doc.styles["Normal"]
    style.font.name = "Calibri"
    style.font.size = Pt(11)

    in_codeblock = False
    codebuf: list[str] = []

    def flush_code() -> None:
        nonlocal codebuf
        if not codebuf:
            return
        p = doc.add_paragraph()
        run = p.add_run("\n".join(codebuf).strip("\n"))
        run.font.name = "Consolas"
        run.font.size = Pt(9)
        codebuf = []

    for raw in lines:
        line = raw.rstrip("\n")

        if line.strip().startswith("```"):
            in_codeblock = not in_codeblock
            if not in_codeblock:
                flush_code()
            continue

        if in_codeblock:
            codebuf.append(raw)
            continue

        if not line.strip():
            continue

        if line.startswith("# "):
            doc.add_heading(line[2:].strip(), level=1)
            continue
        if line.startswith("## "):
            doc.add_heading(line[3:].strip(), level=2)
            continue
        if line.startswith("### "):
            doc.add_heading(line[4:].strip(), level=3)
            continue

        # Tables: render as monospaced rows (best-effort readability).
        if line.lstrip().startswith("|") and line.count("|") >= 2:
            p = doc.add_paragraph()
            run = p.add_run(line)
            run.font.name = "Consolas"
            run.font.size = Pt(9)
            continue

        doc.add_paragraph(line)

    # Ensure headings are left-aligned.
    for para in doc.paragraphs:
        if para.style.name.startswith("Heading"):
            para.alignment = WD_ALIGN_PARAGRAPH.LEFT
            break

    doc.save(out_path)
    print(f"Wrote: {out_path}")


if __name__ == "__main__":
    main()

