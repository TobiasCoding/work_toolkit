#!/usr/bin/env python3
"""
split_pdfs_by_page.py

Uso:
    python split_pdfs_by_page.py /ruta/al/directorio

Descripción:
- Procesa todos los .pdf del directorio dado (no recursivo).
- Crea un archivo por cada página del PDF original.
- El nombre de salida mantiene el nombre base y agrega " - pXXX".
  Ej: "informe.pdf" -> "informe - p001.pdf", "informe - p002.pdf", etc.

Requisitos:
    pip install pypdf
"""

from __future__ import annotations
import argparse
from pathlib import Path
from typing import Iterable
from pypdf import PdfReader, PdfWriter


def split_pdf(pdf_path: Path) -> int:
    """
    Divide un PDF en páginas individuales.
    Retorna la cantidad de páginas exportadas.
    """
    reader = PdfReader(str(pdf_path))

    # Intentar desencriptar PDFs protegidos sin contraseña
    if reader.is_encrypted:
        try:
            reader.decrypt("")  # intenta con password vacía
        except Exception:
            print(f"  [SKIP] Encriptado y no se pudo abrir: {pdf_path.name}")
            return 0

    total_pages = len(reader.pages)
    if total_pages == 0:
        print(f"  [WARN] PDF sin páginas: {pdf_path.name}")
        return 0

    stem = pdf_path.stem  # nombre sin extensión
    parent = pdf_path.parent

    # Generar archivos por página
    for idx in range(total_pages):
        writer = PdfWriter()
        writer.add_page(reader.pages[idx])

        out_name = f"{stem} - p{idx+1:03d}.pdf"
        out_path = parent / out_name

        # Escribir directamente; si existe, sobrescribe.
        with out_path.open("wb") as f:
            writer.write(f)

    return total_pages


def iter_pdfs_in(dir_path: Path) -> Iterable[Path]:
    """Devuelve todos los PDFs (extensión .pdf o .PDF) del directorio (no recursivo)."""
    # glob es case-sensitive en algunos sistemas, por eso filtramos manualmente
    for p in dir_path.iterdir():
        if p.is_file() and p.suffix.lower() == ".pdf":
            yield p


def main() -> None:
    parser = argparse.ArgumentParser(description="Divide todos los PDF del directorio en archivos por página.")
    parser.add_argument("directory", type=Path, help="Directorio que contiene PDFs (no recursivo)")
    args = parser.parse_args()

    base_dir: Path = args.directory
    if not base_dir.exists() or not base_dir.is_dir():
        raise SystemExit(f"Directorio inválido: {base_dir}")

    pdfs = list(iter_pdfs_in(base_dir))
    if not pdfs:
        print("No se encontraron PDFs en el directorio.")
        return

    print(f"Procesando {len(pdfs)} PDF(s) en: {base_dir.resolve()}")
    total_out = 0
    for pdf in pdfs:
        print(f"- {pdf.name}")
        try:
            pages = split_pdf(pdf)
            print(f"  -> {pages} página(s) exportadas")
            total_out += pages
        except Exception as e:
            print(f"  [ERROR] {pdf.name}: {e!s}")

    print(f"Listo. Páginas generadas: {total_out}")


if __name__ == "__main__":
    main()
