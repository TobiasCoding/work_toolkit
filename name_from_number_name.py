#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Renombra archivos .pdf cuyo nombre (sin extensión) es solo un número,
reemplazándolo por el nombre completo según el diccionario `partidos`.

Uso:
    python3 renombrar_partidos_pdf.py /ruta/al/directorio
"""

from __future__ import annotations
import sys
import re
from pathlib import Path

partidos = {
    "1000": "1000 - UNION POR LA REIVINDICACION DE GENERAL ALVARADO",
    "1003": "1003 - CONSTRUYENDO PORVENIR",
    "1004": "1004 - ES AHORA GENERAL ALVARADO",
    "1006": "1006 - PARTIDO LIBERTARIO",
    "1008": "1008 - VALORES REPUBLICANOS",
    "1012": "1012 - HACEMOS LA COSTA",
    "103":  "103 - DEFENSA COMUNAL DE GENERAL PAZ",
    "2200": "2200 - FUERZA PATRIA",
    "2201": "2201 - POTENCIA",
    "2202": "2202 - ES CON VOS ES CON NOSOTROS",
    "2203": "2203 - FRENTE DE IZQUIERDA",
    "2204": "2204 - SOMOS BUENOS AIRES",
    "2205": "2205 - NUEVOS AIRES",
    "2206": "2206 - LA LIBERTAD AVANZA",
    "2207": "2207 - UNION Y LIBERTAD",
    "2208": "2208 - ALIANZA UNION LIBERAL",
    "364":  "364 - ACCION MARPLATENSE",
    "822":  "822 - COMPROMISO VECINAL LA COSTA",
    "869":  "869 - AGRUPACION COMUNAL TRANSFORMADORA",
    "888":  "888 - UNIDAD Y ACCION POR MAR CHIQUITA",
    "895":  "895 - NUEVA NECOCHEA",
    "959":  "959 - MOVIMIENTO AVANZADA SOCIALISTA",
    "963":  "963 - FRENTE PATRIOTA FEDERAL",
    "974":  "974 - POLITICA OBRERA",
    "980":  "980 - TIEMPO DE TODOS",
    "987":  "987 - ALTERNATIVA VECINAL",
    "993":  "993 - SENTIDO COMUN MARPLATENSE",
    "91":   "91 - ESPACIO ABIERTO PARA EL DESARROLLO Y LA INTEGRACIÓN SOCIAL",
}

NUMERIC_STEM = re.compile(r"^\d+$")  # solo dígitos

def main() -> int:
    # Validación básica de argumentos
    if len(sys.argv) != 2:
        print("Uso: python3 renombrar_partidos_pdf.py /ruta/al/directorio", file=sys.stderr)
        return 2

    root = Path(sys.argv[1]).resolve()
    if not root.exists() or not root.is_dir():
        print(f"Error: no existe el directorio: {root}", file=sys.stderr)
        return 2

    total = 0
    renombrados = 0
    saltados_no_mapeado = 0
    saltados_conflicto = 0
    ya_correctos = 0

    # Recorrido recursivo por subdirectorios
    for path in root.rglob("*"):
        # Filtrar solo archivos .pdf (insensible a mayúsculas en la extensión)
        if not path.is_file():
            continue
        if path.suffix.lower() != ".pdf":
            continue

        stem = path.stem  # nombre sin extensión
        # Solo actuar si el nombre es puramente numérico
        if not NUMERIC_STEM.match(stem):
            continue

        total += 1

        # Verificar si el número existe en el diccionario
        nuevo_base = partidos.get(stem)
        if not nuevo_base:
            saltados_no_mapeado += 1
            print(f"· SKIP (no mapeado): {path}")
            continue

        nuevo_nombre = f"{nuevo_base}.pdf"
        destino = path.with_name(nuevo_nombre)

        # Si ya está con el nombre correcto, no hacer nada
        if path.name == nuevo_nombre:
            ya_correctos += 1
            print(f"· OK (ya correcto): {path}")
            continue

        # Evitar sobreescrituras: si existe un conflicto, saltar
        if destino.exists():
            saltados_conflicto += 1
            print(f"· SKIP (conflicto, ya existe): {destino}")
            continue

        # Renombrar
        try:
            path.rename(destino)
            renombrados += 1
            print(f"· RENOMBRADO: {path.name} -> {destino.name}")
        except Exception as e:
            # Cualquier error al renombrar: reportar y continuar
            saltados_conflicto += 1
            print(f"· ERROR al renombrar {path} -> {destino.name}: {e}", file=sys.stderr)

    # Resumen final
    print("\nResumen:")
    print(f"  Candidatos a renombrar (numéricos .pdf): {total}")
    print(f"  Renombrados:                              {renombrados}")
    print(f"  Ya correctos:                             {ya_correctos}")
    print(f"  Saltados (no mapeado):                    {saltados_no_mapeado}")
    print(f"  Saltados (conflicto/errores):             {saltados_conflicto}")

    # Código de salida: 0 si hubo al menos un renombrado o nada que hacer; 1 si todo falló
    if total > 0 and renombrados == 0 and (saltados_conflicto + saltados_no_mapeado) == total:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
