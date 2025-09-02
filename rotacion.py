#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Revisión interactiva de PDFs (1 página) mostrando la imagen ¡en la terminal!

Controles:
  Enter -> siguiente
  1     -> rotar 90° y guardar
  2     -> rotar 180° y guardar
  3     -> rotar 270° y guardar
  4     -> volver al anterior
  q     -> salir

Estrategia de visualización (de mayor a menor calidad):
  kitty icat -> wezterm imgcat -> timg -> viu -> chafa -> ASCII fallback
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from pypdf import PdfReader, PdfWriter
from pdf2image import convert_from_path
from PIL import Image
from pypdf.generic import NameObject, NumberObject


# ------------------------ Utils de visualización ------------------------ #

def _in_path(cmd: str) -> bool:
    return shutil.which(cmd) is not None

def _cols_rows() -> tuple[int, int]:
    try:
        import shutil as _sh
        sz = _sh.get_terminal_size()
        return sz.columns, sz.lines
    except Exception:
        return 120, 40

def show_image_in_terminal(img_path: Path) -> bool:
    """
    Muestra una imagen en la terminal usando la mejor opción disponible.
    Devuelve True si se mostró como bitmap real; False si no fue posible.
    """
    # 1) kitty icat
    if os.environ.get("KITTY_WINDOW_ID") and _in_path("kitty"):
        try:
            subprocess.run(["kitty", "+kitten", "icat", str(img_path)], check=True)
            return True
        except Exception:
            pass

    # 2) wezterm imgcat
    if _in_path("wezterm"):
        try:
            subprocess.run(["wezterm", "imgcat", str(img_path)], check=True)
            return True
        except Exception:
            pass

    # 3) timg (soporta kitty/iterm/sixel/w3m)
    if _in_path("timg"):
        try:
            cols, rows = _cols_rows()
            subprocess.run(["timg", "-g", f"{cols}x{rows}", str(img_path)], check=True)
            return True
        except Exception:
            pass

    # 4) viu (Unicode 24-bit; muy bueno)
    if _in_path("viu"):
        try:
            subprocess.run(["viu", "-n", str(img_path)], check=True)
            return True
        except Exception:
            pass

    # 5) chafa (bloques; muy compatible)
    if _in_path("chafa"):
        try:
            cols, rows = _cols_rows()
            subprocess.run(["chafa", "--size", f"{cols}x{rows}", "--format=symbols", str(img_path)], check=True)
            return True
        except Exception:
            pass

    return False


def show_ascii_fallback(img_path: Path, width: int = 80) -> None:
    """Último recurso: muestra ASCII."""
    img = Image.open(img_path).convert("L")
    w, h = img.size
    ratio = h / w
    new_h = max(1, int(width * ratio * 0.55))
    img = img.resize((width, new_h))
    chars = " .:-=+*#%@"
    px = img.getdata()
    s = "".join(chars[p * (len(chars) - 1) // 255] for p in px)
    lines = [s[i:i + width] for i in range(0, len(s), width)]
    print("\n".join(lines))


# --------------------------- Lógica PDF -------------------------------- #

def pdf_first_page_to_png(pdf_path: Path, dpi: int = 110) -> Path:
    """Convierte la primera página del PDF a PNG temporal."""
    imgs = convert_from_path(str(pdf_path), dpi=dpi, first_page=1, last_page=1)
    out = Path(tempfile.mktemp(suffix=".png"))
    imgs[0].save(out, "PNG")
    return out

def rotate_pdf_inplace(pdf_path: Path, grados: int) -> None:
    """
    Rota la primera página del PDF in-place modificando el atributo /Rotate.
    Acepta 90/180/270 (múltiplos de 90).
    """
    if grados % 90 != 0:
        raise ValueError("La rotación debe ser múltiplo de 90")

    reader = PdfReader(str(pdf_path))
    writer = PdfWriter()

    for idx, page in enumerate(reader.pages):
        if idx == 0:
            # Lee rotación actual (si no existe, 0) y suma la nueva
            actual = page.get("/Rotate", 0) or 0
            try:
                actual = int(actual)
            except Exception:
                actual = 0
            nueva = (actual + grados) % 360
            page[NameObject("/Rotate")] = NumberObject(nueva)

        writer.add_page(page)

    tmp = Path(tempfile.mktemp(suffix=".pdf", dir=str(pdf_path.parent)))
    with open(tmp, "wb") as f:
        writer.write(f)
    os.replace(tmp, pdf_path)



def list_pdfs(root: Path) -> list[Path]:
    return sorted([p for p in root.rglob("*.pdf") if p.is_file()], key=lambda x: str(x).lower())


# ------------------------------ Main ----------------------------------- #

def main():
    ap = argparse.ArgumentParser(description="Revisor/rotador de PDFs (1 página) en la terminal.")
    ap.add_argument("directorio", help="Directorio raíz a recorrer")
    args = ap.parse_args()

    root = Path(args.directorio).expanduser().resolve()
    if not root.is_dir():
        print(f"[ERROR] Directorio inválido: {root}")
        sys.exit(1)

    pdfs = list_pdfs(root)
    if not pdfs:
        print("[INFO] No se encontraron PDFs.")
        return

    i = 0
    total = len(pdfs)
    print("Controles: [Enter]=siguiente  1/2/3=rotar  4=anterior  q=salir")
    while 0 <= i < total:
        pdf = pdfs[i]
        print(f"\n[{i+1}/{total}] {pdf}")

        # Renderizar y mostrar en terminal
        png = None
        try:
            png = pdf_first_page_to_png(pdf)
            if not show_image_in_terminal(png):
                print("[AVISO] Tu terminal no soporta mostrar bitmaps directamente. Fallback a ASCII.")
                show_ascii_fallback(png)
        except Exception as e:
            print(f"[ERROR] No se pudo renderizar {pdf}: {e}")
        finally:
            if png and png.exists():
                try:
                    png.unlink()
                except Exception:
                    pass

        # Input
        acc = input("Acción [Enter/1/2/3/4/q]: ").strip().lower()
        if acc == "q":
            break
        elif acc == "":
            i += 1
        elif acc in {"1", "2", "3"}:
            deg = {"1": 90, "2": 180, "3": 270}[acc]
            try:
                rotate_pdf_inplace(pdf, deg)
                print(f"[OK] Rotado {deg}°")
            except Exception as e:
                print(f"[ERROR] Rotando {pdf}: {e}")
        elif acc == "4":
            i = max(0, i - 1)
        else:
            print("[INFO] Opción inválida.")

    print("\n[LISTO] Proceso terminado.")


if __name__ == "__main__":
    main()
