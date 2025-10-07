# search_systematically.py
# Requiere: PyMuPDF, pyahocorasick, pandas, openpyxl
# Uso (ejemplo):
#   python search_systematically.py --pdf-dir ./reemplazos \
#       --terms-excel ./reporte.xlsx --excel-col "Matrícula" \
#       --match-case sensitive --progress-step 1000 --digits-normalize yes
#
# Comportamiento:
# - Toda la info de procesamiento (INFO/PROGRESO) se imprime primero (stderr).
# - Al finalizar, se imprime SOLO el bloque de "Impresiones" (stdout) para todos los PDFs.
# - Warnings por término duplicado entre PDFs se imprimen al final (stderr).

# USE EXAMPLE:
# python3 .\search_systematically.py --pdf-dir .\reemplazos\ --terms-excel .\reporte_robo_07Oct_para_reimprimir.xlsx --excel-col "Matrícula" --match-case sensitive --progress-step 1000 --digits-normalize yes

import argparse
import os
import sys
import time
import re
from typing import List, Dict, Set, Tuple
import fitz  # PyMuPDF
import ahocorasick
import pandas as pd

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Busca términos en PDF(s) y lista páginas (1-based) agrupadas por 'Impresión' (cada 20 páginas)."
    )
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--pdf", help="Ruta a un archivo PDF.")
    src.add_argument("--pdf-dir", help="Directorio base; busca todos los PDFs recursivamente.")

    terms_src = parser.add_mutually_exclusive_group(required=True)
    terms_src.add_argument("--terms", help='Lista separada por comas. Ej: "a,b,c"')
    terms_src.add_argument("--terms-file", help="Archivo UTF-8 con términos (CSV/líneas).")
    terms_src.add_argument("--terms-excel", help="Archivo Excel (.xlsx/.xls). Requiere --excel-col.")

    parser.add_argument("--excel-col", help="Columna en Excel con los términos (obligatorio si --terms-excel).")
    parser.add_argument("--match-case", required=True, choices=["sensitive","insensitive"], help="Sensibilidad a mayúsculas.")
    parser.add_argument("--progress-step", required=True, type=int, help="Cada cuántas páginas mostrar progreso (>0).")
    parser.add_argument("--digits-normalize", required=True, choices=["yes","no"], help="Si 'yes', quita . - y espacios en texto y términos.")
    args = parser.parse_args()

    if args.terms_excel and not args.excel_col:
        raise ValueError("--excel-col es obligatorio con --terms-excel.")
    if args.progress_step <= 0:
        raise ValueError("--progress-step debe ser > 0.")
    return args

def split_csv_lines(lines: List[str]) -> List[str]:
    """Separa por comas y por líneas; recorta espacios y descarta vacíos."""
    items: List[str] = []
    for line in lines:
        for token in str(line).split(","):
            tok = token.strip()
            if tok:
                items.append(tok)
    return items

def load_terms(args: argparse.Namespace) -> List[str]:
    """Carga términos desde CLI, archivo de texto/CSV o Excel (columna dada)."""
    if args.terms:
        terms = split_csv_lines([args.terms])
    elif args.terms_file:
        with open(args.terms_file, "r", encoding="utf-8") as f:
            terms = split_csv_lines(f.readlines())
    else:
        df = pd.read_excel(args.terms_excel, dtype=str)
        if args.excel_col not in df.columns:
            raise ValueError(f"La columna '{args.excel_col}' no existe en {args.terms_excel}. Columnas: {list(df.columns)}")
        col = df[args.excel_col].dropna().astype(str).tolist()
        terms = [t.strip() for t in split_csv_lines(col) if t.strip()]
    if not terms:
        raise ValueError("La lista de términos está vacía.")
    return terms

def normalize_digits(s: str) -> str:
    """Elimina puntos, espacios y guiones para comparar DNIs/numéricos con formato variable."""
    return re.sub(r"[.\s-]", "", s)

def build_automaton(terms: List[str], case_sensitive: bool, digits_norm: bool) -> ahocorasick.Automaton:
    """Construye autómata Aho-Corasick con normalización elegida."""
    A = ahocorasick.Automaton()
    for idx, term in enumerate(terms):
        key = normalize_digits(term) if digits_norm else (term if case_sensitive else term.lower())
        A.add_word(key, (idx, term))
    A.make_automaton()
    return A

def list_pdfs(base_dir: str) -> List[str]:
    """Lista PDFs recursivamente."""
    pdfs: List[str] = []
    for root, _, files in os.walk(base_dir):
        for name in files:
            if name.lower().endswith(".pdf"):
                pdfs.append(os.path.join(root, name))
    if not pdfs:
        raise ValueError(f"No se encontraron PDFs en: {base_dir}")
    return sorted(pdfs)

def search_in_pdf(pdf_path: str,
                  terms: List[str],
                  A: ahocorasick.Automaton,
                  case_sensitive: bool,
                  digits_norm: bool,
                  progress_step: int) -> Dict[str, Set[int]]:
    """Devuelve dict término -> set de páginas (1-based) encontradas en el PDF."""
    doc = fitz.open(pdf_path)
    total_pages = doc.page_count
    hits: Dict[str, Set[int]] = {t: set() for t in terms}
    t0 = time.time()
    for pno in range(total_pages):
        page = doc.load_page(pno)
        text = page.get_text("text") or ""
        text_for_search = normalize_digits(text) if digits_norm else (text if case_sensitive else text.lower())
        for _, (_, term_orig) in A.iter(text_for_search):
            hits[term_orig].add(pno + 1)
        if (pno + 1) % progress_step == 0 or (pno + 1) == total_pages:
            elapsed = time.time() - t0
            print(f"   > {os.path.basename(pdf_path)} :: página {pno+1}/{total_pages} | {elapsed:.1f}s", file=sys.stderr)
    doc.close()
    return hits

def chunk(seq: List[int], size: int) -> List[List[int]]:
    """Divide una lista en bloques consecutivos de tamaño 'size'."""
    return [seq[i:i+size] for i in range(0, len(seq), size)]

def main():
    args = parse_args()
    terms = load_terms(args)
    case_sensitive = args.match_case == "sensitive"
    digits_norm = args.digits_normalize == "yes"

    print("_"*100)
    print("[PROCESING]:\n")

    # --- Processing info (stderr) ---
    print(f"[INFO] Términos cargados: {len(terms)}", file=sys.stderr)
    pdf_list = [args.pdf] if args.pdf else list_pdfs(args.pdf_dir)
    print(f"[INFO] PDFs a procesar: {len(pdf_list)}", file=sys.stderr)

    print(f"[INFO] Construyendo autómata (case_sensitive={case_sensitive}, digits_norm={digits_norm}) ...", file=sys.stderr)
    A = build_automaton(terms, case_sensitive, digits_norm)
    print("[INFO] Autómata listo.\n", file=sys.stderr)

    base_dir = os.getcwd()

    # Acumular resultados para imprimir "Impresiones" al final
    # impresiones_data: lista de (ruta_rel, [paginas_ordenadas])
    impresiones_data: List[Tuple[str, List[int]]] = []

    # Para WARNING por término duplicado: term -> { pdf_rel_path: [pages] }
    term_to_pdfs: Dict[str, Dict[str, List[int]]] = {t: {} for t in terms}

    # Procesamiento de PDFs (solo logs aquí)
    for pdf_path in pdf_list:
        rel_path = os.path.relpath(pdf_path, start=base_dir)
        print(f"[INFO] Procesando archivo: {rel_path}", file=sys.stderr)

        hits = search_in_pdf(pdf_path, terms, A, case_sensitive, digits_norm, args.progress_step)

        # Unir páginas con al menos un match en este PDF
        union_pages: Set[int] = set()
        for term in terms:
            if hits[term]:
                union_pages.update(hits[term])
                term_to_pdfs[term][rel_path] = sorted(hits[term])

        pages_sorted = sorted(union_pages)
        if pages_sorted:
            impresiones_data.append((rel_path, pages_sorted))

    # --- FIN DEL PROCESAMIENTO / LOGS ---
    # A partir de acá imprimimos SOLO las impresiones (stdout) y warnings (stderr).

    # Impresiones al final, agrupadas de a 20 por archivo:
    print("_"*100)
    print("[PRINTS]:\n")
    idp = 0
    for rel_path, pages_sorted in impresiones_data:
        print(f"{rel_path}")
        for idx, group in enumerate(chunk(pages_sorted, 20), start=1):
            idp +=1
            print(f"> Impresión {idp}: {', '.join(map(str, group))}")
        print("")  # separación entre archivos

    # Warnings por término repetido en >1 PDF
    any_warning = False
    for term, pdf_map in term_to_pdfs.items():
        if len(pdf_map) > 1:
            if not any_warning:
                any_warning = True
            print(f"⚠️  WARNING: El término '{term}' aparece en más de un PDF:", file=sys.stderr)
            for rel_path, pages in pdf_map.items():
                print(f"   - {rel_path}: {', '.join(map(str, pages))}", file=sys.stderr)

    print("_"*100)

    if not impresiones_data:
        print("[INFO] No se encontraron coincidencias en ningún PDF.", file=sys.stderr)
    elif not any_warning:
        print("[INFO] No hay términos repetidos entre PDFs.", file=sys.stderr)

    print("_"*100)

if __name__ == "__main__":
    main()
