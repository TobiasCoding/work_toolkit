# COMANDO EJEMPLO
# python3 unify_pdfs_by_feature.py --target-dir ".\secc1" --p1 7 --p2 9 --index-on original --emit-scope all --optimize full --jpeg-quality 60 --jpeg-min-kb 32
#

# unify_pdfs_by_feature.py
# Unifica PDFs por grupos definidos por una "característica" = substring del nombre (sin extensión)
# tomado por RANGO 1-based [p1..p2], con 2 etapas (descubrir→confirmar→unificar).
# Sin programas externos: compresión con PyMuPDF, incluyendo recompresión de imágenes a JPEG.
#
# Novedades de tamaño (sin pérdida de DPI):
#   - Limpieza y deduplicación (garbage / deflate / object streams).
#   - Subset de fuentes y scrub opcional.
#   - Recompresión interna de imágenes (JPEG) controlada por flags.
#
# Uso típico:
#   python3 unify_pdfs_by_feature.py --target-dir ".\secc1" --p1 7 --p2 9 --index-on original --emit-scope all \
#     --jobs 8 --optimize light --jpeg-recompress on --jpeg-quality 70 --jpeg-min-kb 64
#
# Requisito: pip install pymupdf

import argparse
import os
import sys
import random
from typing import Dict, List, Tuple, Optional
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed

# --------------------------
# Utilidades de consola / UI
# --------------------------

def print_progress(current: int, total: int, prefix: str = "") -> None:
    width = 30
    ratio = 0 if total == 0 else current / total
    filled = int(ratio * width)
    bar = "█" * filled + "·" * (width - filled)
    pct = int(ratio * 100)
    msg = f"\r{prefix} |{bar}| {pct:3d}% ({current}/{total})"
    print(msg, end="", flush=True)
    if current >= total:
        print("")

# --------------------------
# Parsing de argumentos
# --------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Unifica PDFs por característica definida por el RANGO 1-based [p1..p2] del nombre (sin extensión)."
    )
    parser.add_argument("--target-dir", required=True, help="Directorio objetivo con PDFs (no recursivo).")
    parser.add_argument("--p1", required=True, type=int, help="Posición inicial (1-based) dentro del nombre (sin extensión).")
    parser.add_argument("--p2", required=True, type=int, help="Posición final (1-based, inclusiva). Debe ser >= p1.")
    parser.add_argument("--index-on", choices=["original", "filtered"], default="original",
                        help="Dónde se cuentan las posiciones: nombre original o filtrado (default: original).")
    parser.add_argument("--index-filter", choices=["digits", "letters", "alnum", "all"], default="digits",
                        help="[Sólo si --index-on=filtered] Filtro previo para construir el string base (default: digits).")
    parser.add_argument("--emit-scope", choices=["digits", "letters", "alnum", "all"], default="all",
                        help="Filtro a aplicar al substring extraído ANTES de agrupar/nombrar (default: all).")
    parser.add_argument("--out-dir-name", default="unified_files",
                        help="Nombre del subdirectorio destino (por defecto: unified_files).")
    parser.add_argument("--jobs", type=int, default=max(1, multiprocessing.cpu_count() - 1),
                        help="Procesos en paralelo para unificación por grupos (default: CPU-1).")
    parser.add_argument("--optimize", choices=["none", "light", "full"], default="light",
                        help="Nivel de optimización del PDF final vía PyMuPDF (default: light).")
    parser.add_argument("--serial", action="store_true",
                        help="Forzar modo secuencial (debug / evitar paralelismo).")

    # Recompresión interna de imágenes (sin programas externos)
    parser.add_argument("--jpeg-recompress", choices=["on", "off"], default="on",
                        help="Recomprimir imágenes embebidas a JPEG (default: on).")
    parser.add_argument("--jpeg-quality", type=int, default=70,
                        help="Calidad JPEG (60-95 recomendado). Default: 70.")
    parser.add_argument("--jpeg-min-kb", type=int, default=64,
                        help="Tamaño mínimo de imagen (KB) para considerar recompresión. Default: 64.")
    parser.add_argument("--jpeg-only-if-smaller", action="store_true", default=True,
                        help="Sólo reemplazar si el stream recomprimido es más chico (default: True).")

    args = parser.parse_args()

    if args.p1 <= 0 or args.p2 <= 0:
        raise ValueError("--p1 y --p2 deben ser enteros positivos (1-based).")
    if args.p2 < args.p1:
        raise ValueError("--p2 debe ser >= --p1 (rango inclusivo).")

    target = os.path.abspath(args.target_dir)
    if not os.path.isdir(target):
        raise ValueError(f"--target-dir no es un directorio válido: {target}")

    if args.jobs < 1:
        args.jobs = 1

    # Boundaries de calidad
    if args.jpeg_quality < 1 or args.jpeg_quality > 100:
        raise ValueError("--jpeg-quality debe estar entre 1 y 100 (recomendado 60-95).")
    if args.jpeg_min_kb < 0:
        args.jpeg_min_kb = 0

    return args

# --------------------------
# Helpers
# --------------------------

def list_pdfs_flat(dir_path: str) -> List[str]:
    out: List[str] = []
    for name in os.listdir(dir_path):
        full = os.path.join(dir_path, name)
        if os.path.isfile(full) and name.lower().endswith(".pdf"):
            out.append(full)
    out.sort()
    return out

def stem(path: str) -> str:
    base = os.path.basename(path)
    return os.path.splitext(base)[0]

def apply_filter(s: str, mode: str) -> str:
    if mode == "digits":
        return "".join(ch for ch in s if ch.isdigit())
    if mode == "letters":
        return "".join(ch for ch in s if ch.isalpha())
    if mode == "alnum":
        return "".join(ch for ch in s if ch.isalnum())
    return s  # "all"

def compute_feature(name_stem: str, p1: int, p2: int,
                    index_on: str, index_filter: str, emit_scope: str) -> Tuple[str, str]:
    base = name_stem if index_on == "original" else apply_filter(name_stem, index_filter)
    i1 = p1 - 1
    i2 = p2 - 1
    if len(base) <= i2:
        raise IndexError("Longitud insuficiente en la base de indexación para cubrir el rango [p1..p2].")
    slice_raw = base[i1:i2 + 1]
    feature = apply_filter(slice_raw, emit_scope)
    if feature == "":
        feature = slice_raw
    return feature, base

def sanitize_feature(name: str) -> str:
    safe_chars: List[str] = []
    for ch in name:
        if ch.isalnum() or ch in "-_":
            safe_chars.append(ch)
        else:
            safe_chars.append("_")
    out = "".join(safe_chars)
    return out if out else "feature"

# --------------------------
# Recompresión de imágenes (interna)
# --------------------------

def recompress_images_in_doc(doc, quality: int, min_kb: int, only_if_smaller: bool) -> int:
    """
    Reemplaza streams de imágenes por versión JPEG recomprimida con 'quality'.
    No cambia DPI / dimensiones. Retorna cantidad de imágenes reemplazadas.
    """
    import fitz  # PyMuPDF
    replaced = 0
    xref_len = doc.xref_length()
    min_bytes = min_kb * 1024
    # Recorremos xrefs para detectar imágenes
    for xref in range(1, xref_len):
        try:
            t = doc.xref_get_key(xref, "Subtype")[1]
            if t != "/Image":
                continue
        except Exception:
            continue

        # Tamaño actual del stream
        try:
            orig_bytes = len(doc.xref_stream(xref))
        except Exception:
            continue
        if orig_bytes < min_bytes:
            continue  # imágenes muy chicas no valen la pena

        # Armar pixmap desde el xref
        try:
            pix = fitz.Pixmap(doc, xref)
        except Exception:
            continue

        # Convertir a RGB si es CMYK/Indexed/monocromático con máscara
        if pix.n >= 5:  # CMYK u otros con alpha
            try:
                pix = fitz.Pixmap(fitz.csRGB, pix)
            except Exception:
                continue

        # Recomprimir a JPEG (misma resolución, menor calidad)
        try:
            new_bytes = pix.tobytes("jpeg", quality=quality)
        except Exception:
            continue

        # Reemplazar stream si conviene
        if (not only_if_smaller) or (len(new_bytes) < orig_bytes):
            try:
                doc.update_stream(xref, new_bytes)
                replaced += 1
            except Exception:
                pass  # si falla, seguimos

        # liberar memoria del pixmap explícitamente
        try:
            del pix
        except Exception:
            pass

    return replaced

# --------------------------
# Unificación y optimización PyMuPDF
# --------------------------

def unify_group(dest_pdf: str, source_paths: List[str], optimize: str = "light",
                jpeg_recompress: bool = True, jpeg_quality: int = 70, jpeg_min_kb: int = 64,
                jpeg_only_if_smaller: bool = True) -> None:
    # import local dentro del worker (Windows-friendly)
    import fitz  # PyMuPDF
    doc_out = fitz.open()
    try:
        for src in sorted(source_paths):
            with fitz.open(src) as d:
                doc_out.insert_pdf(d, links=False, annots=False, widgets=False)

        # Recompresión interna de imágenes (sin bajar DPI)
        if jpeg_recompress:
            replaced = recompress_images_in_doc(
                doc_out,
                quality=jpeg_quality,
                min_kb=jpeg_min_kb,
                only_if_smaller=jpeg_only_if_smaller
            )
            print(f"[IMG] {os.path.basename(dest_pdf)} -> recomprimidas: {replaced}")

        os.makedirs(os.path.dirname(dest_pdf), exist_ok=True)

        if optimize == "none":
            doc_out.save(dest_pdf, incremental=False)
            return

        if optimize == "light":
            doc_out.save(
                dest_pdf,
                garbage=3,
                clean=True,
                deflate=True,
                use_objstms=1,
                incremental=False
            )
            return

        # optimize == "full" (más chico, más lento)
        doc_out.scrub(
            metadata=True, xml_metadata=True,
            thumbnails=True, attached_files=True, embedded_files=True,
            reset_fields=True, reset_responses=True
        )
        doc_out.subset_fonts()
        doc_out.save(
                dest_pdf,
                garbage=4,
                clean=True,
                deflate=True,
                deflate_images=True,
                deflate_fonts=True,
                use_objstms=1,
                incremental=False
        )
    finally:
        doc_out.close()

# --------------------------
# Worker de proceso (top-level)
# --------------------------

def worker_unify(out_dir: str, feat: str, paths: List[str], optimize: str,
                 jpeg_recompress: bool, jpeg_quality: int, jpeg_min_kb: int, jpeg_only_if_smaller: bool) -> Tuple[str, int, str]:
    try:
        safe_feat = sanitize_feature(feat)
        dest_path = os.path.join(out_dir, f"{safe_feat}.pdf")
        unify_group(
            dest_path, paths, optimize=optimize,
            jpeg_recompress=jpeg_recompress,
            jpeg_quality=jpeg_quality,
            jpeg_min_kb=jpeg_min_kb,
            jpeg_only_if_smaller=jpeg_only_if_smaller
        )
        return (feat, len(paths), "OK")
    except Exception as e:
        return (feat, len(paths), f"ERROR: {e}")

# --------------------------
# Main
# --------------------------

def main():
    args = parse_args()
    target_dir = os.path.abspath(args.target_dir)
    out_dir = os.path.join(target_dir, args.out_dir_name)

    print("=" * 100)
    print("[INFO] Directorio objetivo:", target_dir)
    print(f"[INFO] Rango 1-based: p1 = {args.p1} , p2 = {args.p2} (INCLUSIVO)")
    print("[INFO] Index on:", args.index_on, "| index-filter:",
          args.index_filter if args.index_on == "filtered" else "(n/a)")
    print("[INFO] Emit scope:", args.emit_scope)
    print("[INFO] Directorio de salida:", out_dir)
    print("[INFO] Jobs (procesos):", 1 if args.serial else args.jobs)
    print("[INFO] Optimize:", args.optimize)
    print("[INFO] JPEG recompress:",
          f"ON (quality={args.jpeg_quality}, min_kb={args.jpeg_min_kb}, only_if_smaller={args.jpeg_only_if_smaller})"
          if args.jpeg_recompress == "on" else "OFF")

    pdfs = list_pdfs_flat(target_dir)
    if not pdfs:
        print("[ERROR] No se encontraron PDFs en el directorio objetivo.")
        sys.exit(1)

    print("[INFO] PDFs detectados:", len(pdfs))
    print("-" * 100)

    # ETAPA 1: Descubrimiento
    print("[ETAPA 1] Identificando características y plan de salida...")

    groups: Dict[str, List[str]] = {}
    skipped = 0
    total = len(pdfs)
    for idx, f in enumerate(pdfs, start=1):
        s = stem(f)
        try:
            feat, _ = compute_feature(s, args.p1, args.p2, args.index_on, args.index_filter, args.emit_scope)
        except Exception:
            skipped += 1
        else:
            groups.setdefault(feat, []).append(f)
        print_progress(idx, total, prefix="[Scan]")

    planned_outputs = [f"{sanitize_feature(feat)}.pdf" for feat in sorted(groups.keys())]
    planned_csv = ",".join(planned_outputs)

    print("\n[PLAN] Archivos unificados a crear (CSV):")
    print(planned_csv)
    print(f"[PLAN] Total a crear: {len(planned_outputs)}  |  Características detectadas: {len(groups)}  |  Omitidos por rango: {skipped}")

    print("\n¿Aprobar creación de estos archivos? (s/S/y/Y para continuar) ", end="", flush=True)
    resp = input().strip()
    if resp.lower() not in ("s", "y"):
        print("[CANCELADO] Sin cambios.")
        sys.exit(0)

    # ETAPA 2: Unificación (paralela)
    print("\n[ETAPA 2] Unificando por característica...")
    os.makedirs(out_dir, exist_ok=True)

    feats_sorted = sorted(groups.keys())
    total_groups = len(feats_sorted)
    print_progress(0, total_groups, prefix="[Merge groups]")

    if args.serial or total_groups == 0:
        done = 0
        for feat in feats_sorted:
            paths = groups[feat]
            try:
                worker_unify(out_dir, feat, paths, args.optimize,
                             args.jpeg_recompress == "on", args.jpeg_quality, args.jpeg_min_kb, args.jpeg_only_if_smaller)
                done += 1
                print_progress(done, total_groups, prefix="[Merge groups]")
                print(f"[OK] '{feat}' ({len(paths)} archivo/s)")
            except Exception as e:
                done += 1
                print_progress(done, total_groups, prefix="[Merge groups]")
                print(f"[WARN] Grupo '{feat}' ({len(paths)} archivo/s) => ERROR: {e}")
    else:
        jobs = min(args.jobs, total_groups)
        done = 0
        with ProcessPoolExecutor(max_workers=jobs, mp_context=multiprocessing.get_context("spawn")) as ex:
            futures = []
            for feat in feats_sorted:
                paths = groups[feat]
                futures.append(
                    ex.submit(
                        worker_unify, out_dir, feat, paths, args.optimize,
                        args.jpeg_recompress == "on", args.jpeg_quality, args.jpeg_min_kb, args.jpeg_only_if_smaller
                    )
                )
            for fut in as_completed(futures):
                feat, count, status = fut.result()
                done += 1
                print_progress(done, total_groups, prefix="[Merge groups]")
                if status != "OK":
                    print(f"[WARN] Grupo '{feat}' ({count} archivo/s) => {status}")
                else:
                    print(f"[OK] '{feat}' ({count} archivo/s)")

    print("\n[FIN] PDFs unificados creados:", total_groups)
    print("=" * 100)

if __name__ == "__main__":
    multiprocessing.freeze_support()  # Windows-friendly
    main()
