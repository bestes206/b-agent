"""File and document processing tool — reads text, PDF, and DOCX files."""

import os


def read_file(file_path: str) -> str:
    """Read a file and return its contents as text.

    Supports: .txt, .md, .py, .json, .csv, .pdf, .docx

    Args:
        file_path: Path to the file to read.

    Returns:
        The text content of the file, or an error message.
    """
    if not os.path.exists(file_path):
        return f"File not found: {file_path}"

    ext = os.path.splitext(file_path)[1].lower()

    try:
        if ext == ".pdf":
            return _read_pdf(file_path)
        elif ext == ".docx":
            return _read_docx(file_path)
        else:
            # Treat everything else as plain text
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                return f.read()
    except Exception as e:
        return f"Error reading {file_path}: {e}"


def _read_pdf(file_path: str) -> str:
    """Extract text from a PDF file."""
    from pypdf import PdfReader

    reader = PdfReader(file_path)
    pages = []
    for i, page in enumerate(reader.pages, 1):
        text = page.extract_text() or ""
        pages.append(f"--- Page {i} ---\n{text}")
    return "\n\n".join(pages)


def _read_docx(file_path: str) -> str:
    """Extract text from a DOCX file."""
    from docx import Document

    doc = Document(file_path)
    return "\n".join(p.text for p in doc.paragraphs)


def list_files(directory: str = ".") -> str:
    """List files in a directory.

    Args:
        directory: Path to the directory (default: current directory).

    Returns:
        A formatted listing of files and subdirectories.
    """
    if not os.path.isdir(directory):
        return f"Not a directory: {directory}"

    entries = sorted(os.listdir(directory))
    lines = []
    for entry in entries:
        full_path = os.path.join(directory, entry)
        if os.path.isdir(full_path):
            lines.append(f"  [DIR]  {entry}/")
        else:
            size = os.path.getsize(full_path)
            lines.append(f"  [FILE] {entry} ({size:,} bytes)")
    return "\n".join(lines) if lines else "Directory is empty."
