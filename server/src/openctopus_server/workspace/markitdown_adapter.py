from __future__ import annotations

import re
from importlib import import_module
from io import BytesIO
from typing import Literal

from markitdown import StreamInfo
from markitdown.converters import (
    DocxConverter,
    HtmlConverter,
    PdfConverter,
    PptxConverter,
    XlsxConverter,
)

type ConversionFormat = Literal["pdf", "docx", "xlsx", "pptx", "html"]

_STREAM_INFO: dict[ConversionFormat, StreamInfo] = {
    "pdf": StreamInfo(extension=".pdf", mimetype="application/pdf"),
    "docx": StreamInfo(
        extension=".docx",
        mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ),
    "xlsx": StreamInfo(
        extension=".xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ),
    "pptx": StreamInfo(
        extension=".pptx",
        mimetype="application/vnd.openxmlformats-officedocument.presentationml.presentation",
    ),
    "html": StreamInfo(extension=".html", mimetype="text/html", charset="utf-8"),
}


def convert_bytes(data: bytes, conversion_format: ConversionFormat) -> str:
    converter = _converter(conversion_format)
    result = converter.convert(
        BytesIO(data),
        _STREAM_INFO[conversion_format],
    )
    return _normalize_markdown(str(result.text_content))


def probe_dependencies() -> None:
    for module_name in (
        "mammoth",
        "openpyxl",
        "pandas",
        "pdfminer.high_level",
        "pdfplumber",
        "pptx",
        "pypdf",
    ):
        import_module(module_name)
    for conversion_format in _STREAM_INFO:
        _converter(conversion_format)


def _normalize_markdown(value: str) -> str:
    value = "\n".join(line.rstrip() for line in re.split(r"\r?\n", value))
    return re.sub(r"\n{3,}", "\n\n", value)


def _converter(
    conversion_format: ConversionFormat,
) -> PdfConverter | DocxConverter | XlsxConverter | PptxConverter | HtmlConverter:
    if conversion_format == "pdf":
        return PdfConverter()
    if conversion_format == "docx":
        return DocxConverter()  # type: ignore[no-untyped-call]
    if conversion_format == "xlsx":
        return XlsxConverter()  # type: ignore[no-untyped-call]
    if conversion_format == "pptx":
        return PptxConverter()  # type: ignore[no-untyped-call]
    return HtmlConverter()
