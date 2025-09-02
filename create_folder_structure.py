#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crea carpetas a partir de un Excel con columnas 'carpeta1' y 'carpeta2'.

Uso (sin valores por defecto):
    python create_folder_structure_from_excel.py /ruta/base /ruta/archivo.xlsx

Ejemplo:
    python create_folder_structure_from_excel.py /home/usuario/proyecto secciones.xlsx

Requisitos:
    - Python 3.10+
    - pandas y openpyxl (para leer .xlsx)
      pip install pandas openpyxl
"""

from pathlib import Path
import sys
import pandas as pd

def _err(msg: str, code: int) -> "NoReturn":  # tipo para claridad, evita valores por defecto
    print(f"Error: {msg}", file=sys.stderr)
    sys.exit(code)

def main() -> None:
    # Validación estricta de argumentos
    if len(sys.argv) != 3:
        print("Uso: python create_carpetas_from_excel.py /ruta/base /ruta/archivo.xlsx", file=sys.stderr)
        sys.exit(2)

    base_dir = Path(sys.argv[1]).expanduser().resolve()
    excel_path = Path(sys.argv[2]).expanduser().resolve()

    # Validaciones de ruta
    if not base_dir.exists() or not base_dir.is_dir():
        _err(f"el directorio base no existe o no es un directorio: {base_dir}", 3)
    if not excel_path.exists() or not excel_path.is_file():
        _err(f"el archivo Excel no existe: {excel_path}", 4)
    if excel_path.suffix.lower() != ".xlsx":
        _err("solo se admite formato .xlsx", 5)

    # Lectura del Excel sin inferencias “mágicas”
    try:
        df = pd.read_excel(excel_path, engine="openpyxl", dtype=str)
    except Exception as e:
        _err(f"no se pudo leer el Excel: {e}", 6)

    # Validar encabezados EXACTOS
    expected_cols = {"carpeta1", "carpeta2"}
    cols_lower = {c.lower(): c for c in df.columns}
    if not expected_cols.issubset(cols_lower.keys()):
        _err(f"el archivo debe tener encabezados EXACTOS: {sorted(expected_cols)}; encontrados: {list(df.columns)}", 7)

    # Normalizar a str estricta y trim
    df = df.assign(
        carpeta1=df[cols_lower["carpeta1"]].astype(str).str.strip(),
        carpeta2=df[cols_lower["carpeta2"]].astype(str).str.strip(),
    )

    # Validar filas vacías
    invalid = df[(df["carpeta1"] == "") | (df["carpeta2"] == "")]
    if not invalid.empty:
        _err(f"hay filas con 'carpeta1' o 'carpeta2' vacías. Corrigí el Excel y reintentá.", 8)

    # Crear carpetas
    created = 0
    for _, row in df.iterrows():
        parent = row["carpeta1"]
        child = row["carpeta2"]
        target = base_dir / parent / child
        target.mkdir(parents=True, exist_ok=True)
        print(f"OK  -> {target}")
        created += 1

    print(f"\nListo. Se aseguraron {created} carpetas (par hijo) bajo: {base_dir}")

if __name__ == "__main__":
    main()
