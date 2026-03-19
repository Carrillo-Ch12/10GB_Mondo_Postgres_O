#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ejecuta pg_squeeze real sobre tpch_squeeze.
Requisitos previos:
  - wal_level = logical  (ALTER SYSTEM SET wal_level = logical + restart)
  - pg_squeeze instalado (ya instalado en tpch_squeeze)

Pasos por tabla:
  1. Agregar NOT NULL en columnas clave
  2. Crear indice unico NOT NULL (requerido por REPLICA IDENTITY)
  3. Asignar REPLICA IDENTITY USING INDEX
  4. Ejecutar squeeze.squeeze_table()
  5. Restaurar REPLICA IDENTITY DEFAULT
  6. Eliminar indice temporal
"""

import sys
import time
from datetime import datetime
import psycopg2

# ============================================================
# CONFIG
# ============================================================

PG_PORT       = 5432
PG_USER       = "postgres"
DATABASE_NAME = "tpch_squeeze"

# Orden: pequeñas primero, lineitem al final
TABLES = [
    {
        "name":     "region",
        "idx":      "sq_idx_region",
        "idx_ddl":  "CREATE UNIQUE INDEX sq_idx_region ON region (r_regionkey)",
        "not_null": ["r_regionkey"],
    },
    {
        "name":     "nation",
        "idx":      "sq_idx_nation",
        "idx_ddl":  "CREATE UNIQUE INDEX sq_idx_nation ON nation (n_nationkey)",
        "not_null": ["n_nationkey"],
    },
    {
        "name":     "supplier",
        "idx":      "sq_idx_supplier",
        "idx_ddl":  "CREATE UNIQUE INDEX sq_idx_supplier ON supplier (s_suppkey)",
        "not_null": ["s_suppkey"],
    },
    {
        "name":     "part",
        "idx":      "sq_idx_part",
        "idx_ddl":  "CREATE UNIQUE INDEX sq_idx_part ON part (p_partkey)",
        "not_null": ["p_partkey"],
    },
    {
        "name":     "customer",
        "idx":      "sq_idx_customer",
        "idx_ddl":  "CREATE UNIQUE INDEX sq_idx_customer ON customer (c_custkey)",
        "not_null": ["c_custkey"],
    },
    {
        "name":     "partsupp",
        "idx":      "sq_idx_partsupp",
        "idx_ddl":  "CREATE UNIQUE INDEX sq_idx_partsupp ON partsupp (ps_partkey, ps_suppkey)",
        "not_null": ["ps_partkey", "ps_suppkey"],
    },
    {
        "name":     "orders",
        "idx":      "sq_idx_orders",
        "idx_ddl":  "CREATE UNIQUE INDEX sq_idx_orders ON orders (o_orderkey)",
        "not_null": ["o_orderkey"],
    },
    {
        "name":     "lineitem",
        "idx":      "sq_idx_lineitem",
        "idx_ddl":  "CREATE UNIQUE INDEX sq_idx_lineitem ON lineitem (l_orderkey, l_linenumber)",
        "not_null": ["l_orderkey", "l_linenumber"],
    },
]

# ============================================================
# Helpers
# ============================================================

def connect():
    conn = psycopg2.connect(
        host="/var/run/postgresql",
        port=PG_PORT,
        user=PG_USER,
        dbname=DATABASE_NAME
    )
    conn.autocommit = True  # OBLIGATORIO para pg_squeeze
    return conn


def get_size(cur, table):
    cur.execute(f"SELECT pg_size_pretty(pg_total_relation_size('{table}'))")
    return cur.fetchone()[0]


def check_prerequisites(cur):
    """Verifica wal_level y que pg_squeeze este instalado."""
    cur.execute("SHOW wal_level")
    wal = cur.fetchone()[0]
    print(f"  wal_level        : {wal}")
    if wal != "logical":
        print("\n  [ERROR] wal_level debe ser 'logical'.")
        print("  Ejecuta estos comandos y vuelve a correr el script:")
        print("    sudo -u postgres psql -c \"ALTER SYSTEM SET wal_level = logical;\"")
        print("    sudo systemctl restart postgresql")
        return False

    cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'pg_squeeze'")
    row = cur.fetchone()
    if not row:
        print("  [ERROR] pg_squeeze no esta instalado en esta base de datos.")
        return False
    print(f"  pg_squeeze       : v{row[0]}")
    return True


def squeeze_table(cur, table_cfg):
    """
    Ejecuta pg_squeeze en una tabla.
    Retorna True si squeeze fue exitoso, False si fallo.
    """
    name     = table_cfg["name"]
    idx      = table_cfg["idx"]
    idx_ddl  = table_cfg["idx_ddl"]
    not_null = table_cfg["not_null"]

    print(f"\n{'='*60}")
    print(f"  Tabla: {name.upper()}")
    print(f"{'='*60}")

    # Paso 1 — NOT NULL en columnas clave
    print(f"  [1/5] Aplicando NOT NULL en columnas clave...")
    try:
        for col in not_null:
            cur.execute(f"ALTER TABLE {name} ALTER COLUMN {col} SET NOT NULL")
        print(f"        OK: {', '.join(not_null)}")
    except Exception as e:
        print(f"        [ERROR] {e}")
        return False

    # Paso 2 — Crear indice unico temporal
    print(f"  [2/5] Creando indice unico temporal ({idx})...")
    try:
        cur.execute(f"DROP INDEX IF EXISTS {idx}")
        cur.execute(idx_ddl)
        print(f"        OK")
    except Exception as e:
        print(f"        [ERROR] {e}")
        return False

    # Paso 3 — Asignar REPLICA IDENTITY
    print(f"  [3/5] Configurando REPLICA IDENTITY USING INDEX {idx}...")
    try:
        cur.execute(f"ALTER TABLE {name} REPLICA IDENTITY USING INDEX {idx}")
        print(f"        OK")
    except Exception as e:
        print(f"        [ERROR] {e}")
        cur.execute(f"DROP INDEX IF EXISTS {idx}")
        return False

    # Paso 4 — Ejecutar squeeze
    print(f"  [4/5] Ejecutando squeeze.squeeze_table()...")
    t0      = time.time()
    success = False
    try:
        cur.execute(f"SELECT squeeze.squeeze_table('public', '{name}', '{idx}')")
        elapsed = time.time() - t0
        print(f"        OK — {elapsed:.1f}s")
        success = True
    except Exception as e:
        elapsed = time.time() - t0
        print(f"        [ERROR] squeeze fallo ({elapsed:.1f}s): {e}")

    # Paso 5 — Limpiar: restaurar REPLICA IDENTITY y eliminar indice
    print(f"  [5/5] Limpiando (REPLICA IDENTITY DEFAULT + DROP INDEX)...")
    try:
        cur.execute(f"ALTER TABLE {name} REPLICA IDENTITY DEFAULT")
        cur.execute(f"DROP INDEX IF EXISTS {idx}")
        print(f"        OK")
    except Exception as e:
        print(f"        [WARN] Limpieza parcial: {e}")

    return success


# ============================================================
# Main
# ============================================================

def main():
    print("=" * 60)
    print("PG_SQUEEZE — COMPACTACION REAL DE tpch_squeeze")
    print(f"Fecha : {datetime.now().isoformat(timespec='seconds')}")
    print("=" * 60)

    try:
        conn = connect()
        cur  = conn.cursor()
        print(f"[OK] Conectado a {DATABASE_NAME}\n")
    except Exception as e:
        print(f"[ERROR] Conexion fallida: {e}")
        sys.exit(1)

    # Verificar prerequisitos
    print("[VERIFICACION]")
    if not check_prerequisites(cur):
        sys.exit(1)

    # Tamaños antes
    print("\n[TAMAÑOS ANTES]")
    sizes_antes = {}
    for t in TABLES:
        s = get_size(cur, t["name"])
        sizes_antes[t["name"]] = s
        print(f"  {t['name']:<12}: {s}")

    # Ejecutar squeeze tabla por tabla
    print("\n[SQUEEZE]")
    ok_list   = []
    fail_list = []

    for t in TABLES:
        result = squeeze_table(cur, t)
        if result:
            ok_list.append(t["name"])
        else:
            fail_list.append(t["name"])

    # Tamaños después
    print(f"\n{'='*60}")
    print("[TAMAÑOS DESPUES]")
    for t in TABLES:
        antes   = sizes_antes[t["name"]]
        despues = get_size(cur, t["name"])
        cambio  = " ←" if antes != despues else ""
        print(f"  {t['name']:<12}: {antes:>10}  →  {despues:>10}{cambio}")

    cur.execute(f"SELECT pg_size_pretty(pg_database_size('{DATABASE_NAME}'))")
    total = cur.fetchone()[0]
    print(f"\n  Total BD: {total}")

    # Resumen
    print(f"\n{'='*60}")
    print("RESUMEN FINAL")
    print(f"{'='*60}")
    print(f"  Tablas OK      : {len(ok_list)}/{len(TABLES)}  →  {', '.join(ok_list) if ok_list else 'ninguna'}")
    if fail_list:
        print(f"  Tablas fallidas: {len(fail_list)}/{len(TABLES)}  →  {', '.join(fail_list)}")

    if len(ok_list) == len(TABLES):
        print(f"\n[LISTO] pg_squeeze aplicado correctamente en {DATABASE_NAME} ✓")
    elif ok_list:
        print(f"\n[PARCIAL] pg_squeeze aplicado en {len(ok_list)}/{len(TABLES)} tablas.")
    else:
        print(f"\n[FALLO] pg_squeeze no pudo aplicarse en ninguna tabla.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.")
    except Exception as e:
        print(f"\n[ERROR FATAL] {e}")
        sys.exit(1)
