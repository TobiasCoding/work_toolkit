#!/usr/bin/env python3
# compare_tables_in_excel_sheet_by_id_estab.py
# Compara dos hojas de un archivo Excel por la clave ID_ESTAB.
# - Detecta IDs solo en A, solo en B.
# - Detecta diferencias campo a campo para IDs presentes en ambas.
# Uso:
#   python compare_tables_in_excel_sheet_by_id_estab.py input.xlsx \
#       --sheet-a "Hoja1" --sheet-b "Hoja2" \
#       --id-col "ID_ESTAB" --out-dir "salida"
#
# Si no indicás --sheet-a/--sheet-b, toma las dos primeras hojas.
# Requisitos: pandas, openpyxl (para .xlsx)

import argparse
import os
import sys
from typing import List, Tuple
import pandas as pd


def normalize_id(series: pd.Series) -> pd.Series:
    """
    Normaliza ID: convierte a str, recorta espacios, y reemplaza NaN por cadena vacía.
    """
    return (
        series.astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)  # quita .0 de IDs numéricos convertidos
        .fillna("")
    )


def read_two_sheets(
    path: str, sheet_a: str | None, sheet_b: str | None
) -> Tuple[pd.DataFrame, pd.DataFrame, str, str]:
    """
    Lee dos hojas de un Excel. Si no se especifican nombres, usa las dos primeras.
    Devuelve df_a, df_b, name_a, name_b.
    """
    xl = pd.ExcelFile(path)
    sheets = xl.sheet_names
    if sheet_a is None or sheet_b is None:
        if len(sheets) < 2:
            print("Error: el archivo no tiene al menos dos hojas.", file=sys.stderr)
            sys.exit(2)
        if sheet_a is None:
            sheet_a = sheets[0]
        if sheet_b is None:
            sheet_b = sheets[1]
    df_a = pd.read_excel(path, sheet_name=sheet_a, dtype=object)
    df_b = pd.read_excel(path, sheet_name=sheet_b, dtype=object)
    return df_a, df_b, sheet_a, sheet_b


def ensure_id_column(df: pd.DataFrame, id_col: str, label: str) -> pd.DataFrame:
    """
    Verifica que exista la columna id_col y la normaliza.
    """
    if id_col not in df.columns:
        print(f"Error: en {label} no existe la columna '{id_col}'.", file=sys.stderr)
        sys.exit(2)
    df = df.copy()
    df[id_col] = normalize_id(df[id_col])
    # Elimina filas con ID vacío (opcional, para evitar falsos positivos)
    df = df[df[id_col] != ""]
    return df


def drop_duplicate_ids(df: pd.DataFrame, id_col: str, label: str) -> pd.DataFrame:
    """
    Si hay IDs duplicados, avisa y conserva la primera aparición.
    """
    dup_mask = df.duplicated(subset=[id_col], keep="first")
    n_dups = dup_mask.sum()
    if n_dups > 0:
        print(
            f"Aviso: {label} tiene {n_dups} IDs duplicados en '{id_col}'. "
            f"Se conservará la primera ocurrencia.",
            file=sys.stderr,
        )
        df = df[~dup_mask].copy()
    return df


def diff_common_columns(
    left: pd.DataFrame, right: pd.DataFrame, id_col: str, label_a: str, label_b: str
) -> pd.DataFrame:
    """
    Para IDs presentes en ambas tablas, compara solo las columnas comunes (excepto el id).
    Devuelve un DataFrame largo con diferencias: ID_ESTAB, columna, valor_A, valor_B.
    """
    common_cols = [c for c in left.columns.intersection(right.columns) if c != id_col]
    if not common_cols:
        return pd.DataFrame(columns=[id_col, "columna", "valor_A", "valor_B"])

    # Asegura mismo orden y tipos comparables
    L = left[[id_col] + common_cols].copy()
    R = right[[id_col] + common_cols].copy()

    # Igualamos tipos a string para comparación robusta (evita NaN != '' etc.)
    for c in common_cols:
        L[c] = L[c].astype(str)
        R[c] = R[c].astype(str)

    merged = L.merge(R, on=id_col, how="inner", suffixes=("_A", "_B"))

    diffs = []
    for col in common_cols:
        a = f"{col}_A"
        b = f"{col}_B"
        neq_mask = merged[a].fillna("") != merged[b].fillna("")
        if neq_mask.any():
            tmp = merged.loc[neq_mask, [id_col, a, b]].copy()
            tmp["columna"] = col
            tmp = tmp.rename(columns={a: "valor_A", b: "valor_B"})
            # Reordenamos columnas
            tmp = tmp[[id_col, "columna", "valor_A", "valor_B"]]
            diffs.append(tmp)

    if diffs:
        return pd.concat(diffs, ignore_index=True)
    else:
        return pd.DataFrame(columns=[id_col, "columna", "valor_A", "valor_B"])


def main():
    parser = argparse.ArgumentParser(
        description="Compara dos hojas de un Excel por ID_ESTAB y reporta diferencias."
    )
    parser.add_argument("excel", help="Ruta al archivo .xlsx")
    parser.add_argument("--sheet-a", help="Nombre de la hoja A (por defecto, 1ra hoja)")
    parser.add_argument("--sheet-b", help="Nombre de la hoja B (por defecto, 2da hoja)")
    parser.add_argument(
        "--id-col",
        default="ID_ESTAB",
        help="Nombre de la columna clave (default: ID_ESTAB)",
    )
    parser.add_argument(
        "--out-dir",
        default="comparacion_salida",
        help="Directorio de salida para CSVs",
    )
    args = parser.parse_args()

    # Lee hojas
    df_a, df_b, name_a, name_b = read_two_sheets(args.excel, args.sheet_a, args.sheet_b)

    # Verifica/normaliza ID
    df_a = ensure_id_column(df_a, args.id_col, f"Hoja '{name_a}'")
    df_b = ensure_id_column(df_b, args.id_col, f"Hoja '{name_b}'")

    # Maneja duplicados de ID
    df_a = drop_duplicate_ids(df_a, args.id_col, f"Hoja '{name_a}'")
    df_b = drop_duplicate_ids(df_b, args.id_col, f"Hoja '{name_b}'")

    # Índices por ID para merges rápidos
    # (No es obligatorio, pero ayuda a dejar claro que la clave es única)
    df_a = df_a.set_index(args.id_col, drop=False)
    df_b = df_b.set_index(args.id_col, drop=False)

    # IDs solo en A y solo en B
    ids_a = set(df_a[args.id_col].tolist())
    ids_b = set(df_b[args.id_col].tolist())
    only_a = sorted(ids_a - ids_b)
    only_b = sorted(ids_b - ids_a)
    both = sorted(ids_a & ids_b)

    # Arma reportes de presencia/ausencia
    only_a_df = pd.DataFrame({args.id_col: only_a})
    if not only_a_df.empty:
        only_a_df["en"] = name_a
        only_a_df["no_en"] = name_b

    only_b_df = pd.DataFrame({args.id_col: only_b})
    if not only_b_df.empty:
        only_b_df["en"] = name_b
        only_b_df["no_en"] = name_a

    # Diferencias en columnas comunes para IDs presentes en ambas
    diffs_df = diff_common_columns(
        df_a.reset_index(drop=True),
        df_b.reset_index(drop=True),
        args.id_col,
        name_a,
        name_b,
    )

    # Salida
    os.makedirs(args.out_dir, exist_ok=True)
    path_only_a = os.path.join(args.out_dir, "ids_solo_en_A.csv")
    path_only_b = os.path.join(args.out_dir, "ids_solo_en_B.csv")
    path_diffs = os.path.join(args.out_dir, "diferencias_en_comunes.csv")

    (only_a_df if not only_a_df.empty else pd.DataFrame(columns=[args.id_col, "en", "no_en"])).to_csv(
        path_only_a, index=False
    )
    (only_b_df if not only_b_df.empty else pd.DataFrame(columns=[args.id_col, "en", "no_en"])).to_csv(
        path_only_b, index=False
    )
    (diffs_df if not diffs_df.empty else pd.DataFrame(columns=[args.id_col, "columna", "valor_A", "valor_B"])).to_csv(
        path_diffs, index=False
    )

    # Resumen por consola
    print("=== Resumen comparación por ID ===")
    print(f"Archivo: {args.excel}")
    print(f"Hojas: A='{name_a}'  B='{name_b}'")
    print(f"Columna clave: {args.id_col}")
    print(f"IDs solo en A: {len(only_a)}  -> {path_only_a}")
    print(f"IDs solo en B: {len(only_b)}  -> {path_only_b}")
    print(f"Diferencias en columnas comunes (IDs en ambas): {len(diffs_df)} filas -> {path_diffs}")
    if len(only_a) > 0:
        print(f"  Ejemplo solo en A: {only_a[:5]}")
    if len(only_b) > 0:
        print(f"  Ejemplo solo en B: {only_b[:5]}")
    if not diffs_df.empty:
        print("  Ejemplo de diferencia:")
        print(diffs_df.head(5).to_string(index=False))


if __name__ == "__main__":
    pd.set_option("display.max_columns", None)
    main()
