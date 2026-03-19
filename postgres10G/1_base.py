#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crea BD base TPC-H en PostgreSQL: sin optimizaciones
Sin compresion, sin indices adicionales (solo PKs)
Nodo centralizado: localhost
"""

import os
import sys
import time
import io
from datetime import datetime
import psycopg2
from psycopg2 import sql

# ============================================================
# CONFIG
# ============================================================

TPCH_DATA_DIR = "/home/ana-aguilera/Rodolfo/datos/tpc-h"
DATABASE_NAME = "tpch_base"
PG_HOST       = "localhost"
PG_PORT       = 5432
PG_USER       = "postgres"

# ============================================================
# DDL tablas TPC-H
# ============================================================

TABLES_DDL = {
    "region": """
        CREATE TABLE IF NOT EXISTS region (
            r_regionkey  INTEGER,
            r_name       TEXT,
            r_comment    TEXT
        )
    """,
    "nation": """
        CREATE TABLE IF NOT EXISTS nation (
            n_nationkey  INTEGER,
            n_name       TEXT,
            n_regionkey  INTEGER,
            n_comment    TEXT
        )
    """,
    "customer": """
        CREATE TABLE IF NOT EXISTS customer (
            c_custkey    INTEGER,
            c_name       TEXT,
            c_address    TEXT,
            c_nationkey  INTEGER,
            c_phone      TEXT,
            c_acctbal    DOUBLE PRECISION,
            c_mktsegment TEXT,
            c_comment    TEXT
        )
    """,
    "supplier": """
        CREATE TABLE IF NOT EXISTS supplier (
            s_suppkey    INTEGER,
            s_name       TEXT,
            s_address    TEXT,
            s_nationkey  INTEGER,
            s_phone      TEXT,
            s_acctbal    DOUBLE PRECISION,
            s_comment    TEXT
        )
    """,
    "part": """
        CREATE TABLE IF NOT EXISTS part (
            p_partkey     INTEGER,
            p_name        TEXT,
            p_mfgr        TEXT,
            p_brand       TEXT,
            p_type        TEXT,
            p_size        INTEGER,
            p_container   TEXT,
            p_retailprice DOUBLE PRECISION,
            p_comment     TEXT
        )
    """,
    "partsupp": """
        CREATE TABLE IF NOT EXISTS partsupp (
            ps_partkey    INTEGER,
            ps_suppkey    INTEGER,
            ps_availqty   INTEGER,
            ps_supplycost DOUBLE PRECISION,
            ps_comment    TEXT
        )
    """,
    "orders": """
        CREATE TABLE IF NOT EXISTS orders (
            o_orderkey      INTEGER,
            o_custkey       INTEGER,
            o_orderstatus   TEXT,
            o_totalprice    DOUBLE PRECISION,
            o_orderdate     DATE,
            o_orderpriority TEXT,
            o_clerk         TEXT,
            o_shippriority  INTEGER,
            o_comment       TEXT
        )
    """,
    "lineitem": """
        CREATE TABLE IF NOT EXISTS lineitem (
            l_orderkey      INTEGER,
            l_partkey       INTEGER,
            l_suppkey       INTEGER,
            l_linenumber    INTEGER,
            l_quantity      DOUBLE PRECISION,
            l_extendedprice DOUBLE PRECISION,
            l_discount      DOUBLE PRECISION,
            l_tax           DOUBLE PRECISION,
            l_returnflag    TEXT,
            l_linestatus    TEXT,
            l_shipdate      DATE,
            l_commitdate    DATE,
            l_receiptdate   DATE,
            l_shipinstruct  TEXT,
            l_shipmode      TEXT,
            l_comment       TEXT
        )
    """,
}

# Columnas TEXT por tabla para deshabilitar compresion TOAST
TABLES_TEXT_COLS = {
    "region":   ["r_name", "r_comment"],
    "nation":   ["n_name", "n_comment"],
    "customer": ["c_name", "c_address", "c_phone", "c_mktsegment", "c_comment"],
    "supplier": ["s_name", "s_address", "s_phone", "s_comment"],
    "part":     ["p_name", "p_mfgr", "p_brand", "p_type", "p_container", "p_comment"],
    "partsupp": ["ps_comment"],
    "orders":   ["o_orderstatus", "o_orderpriority", "o_clerk", "o_comment"],
    "lineitem": ["l_returnflag", "l_linestatus", "l_shipinstruct", "l_shipmode", "l_comment"],
}

TABLES_FILES = {
    "region":   "region.tbl",
    "nation":   "nation.tbl",
    "customer": "customer.tbl",
    "supplier": "supplier.tbl",
    "part":     "part.tbl",
    "partsupp": "partsupp.tbl",
    "orders":   "orders.tbl",
    "lineitem": "lineitem.tbl",
}

# ============================================================
# Helpers
# ============================================================

def connect_postgres(dbname="postgres"):
    return psycopg2.connect(
        host="/var/run/postgresql",
        port=PG_PORT,
        user=PG_USER,
        dbname=dbname
    )


def create_database(conn):
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DATABASE_NAME,))
    if cur.fetchone():
        print(f"  [INFO] Base {DATABASE_NAME} ya existe, se eliminara y recreara.")
        cur.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(DATABASE_NAME)))
    cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DATABASE_NAME)))
    print(f"  [OK] Base de datos {DATABASE_NAME} creada.")
    cur.close()


def create_tables(conn):
    cur = conn.cursor()
    for table, ddl in TABLES_DDL.items():
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(ddl)
        for col in TABLES_TEXT_COLS.get(table, []):
            cur.execute(f"ALTER TABLE {table} ALTER COLUMN {col} SET STORAGE EXTERNAL")
        print(f"  [OK] {table} (TEXT sin compresion TOAST)")
    conn.commit()
    cur.close()


def load_table_copy(conn, table: str, fpath: str) -> int:
    cur   = conn.cursor()
    total = 0

    with open(fpath, "r", encoding="utf-8") as f:
        clean = io.StringIO()
        for line in f:
            line = line.rstrip("\n").rstrip("|")
            clean.write(line + "\n")
            total += 1
        clean.seek(0)

    cur.copy_from(clean, table, sep="|", null="")
    conn.commit()
    cur.close()
    return total


def verify_table(conn, table: str) -> int:
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    count = cur.fetchone()[0]
    cur.close()
    return count


# ============================================================
# Main
# ============================================================

def main():
    print("=" * 70)
    print("SETUP TPC-H BASE - POSTGRESQL - NODO CENTRALIZADO")
    print(f"Fecha    : {datetime.now().isoformat(timespec='seconds')}")
    print(f"DB       : {DATABASE_NAME}")
    print(f"Host     : {PG_HOST}:{PG_PORT}")
    print(f"Datos    : {TPCH_DATA_DIR}")
    print(f"Compres. : NINGUNA (STORAGE EXTERNAL en columnas TEXT)")
    print(f"Indices  : NO (sin indices adicionales)")
    print("=" * 70)

    try:
        conn_admin = connect_postgres(dbname="postgres")
        print(f"[OK] Conectado a PostgreSQL {PG_HOST}:{PG_PORT}")
    except Exception as e:
        print(f"[ERROR] No se pudo conectar a PostgreSQL: {e}")
        sys.exit(1)

    print("\n[1/3] Creando base de datos...")
    create_database(conn_admin)
    conn_admin.close()

    conn = connect_postgres(dbname=DATABASE_NAME)

    print("\n[2/3] Creando tablas (sin compresion TOAST)...")
    create_tables(conn)

    print("\n[3/3] Cargando datos desde archivos .tbl...")
    results = []
    start   = time.time()

    for table, fname in TABLES_FILES.items():
        print(f"\n{'='*70}")
        print(f"[CARGA] {table.upper()}")
        print(f"{'='*70}")
        fpath = os.path.join(TPCH_DATA_DIR, fname)
        if not os.path.exists(fpath):
            print(f"  [ERROR] No existe: {fpath}")
            results.append((table, 0, False))
            continue
        t0 = time.time()
        try:
            total    = load_table_copy(conn, table, fpath)
            elapsed  = time.time() - t0
            verified = verify_table(conn, table)
            print(f"  [OK] {verified:,} filas cargadas ({elapsed:.1f}s)")
            results.append((table, verified, True))
        except Exception as e:
            print(f"  [ERROR] {e}")
            conn.rollback()
            results.append((table, 0, False))

    elapsed_total = (time.time() - start) / 60

    print("\n" + "=" * 70)
    print("RESUMEN FINAL")
    print("=" * 70)
    print(f"Tiempo total : {elapsed_total:.1f} min")
    for table, total, ok in results:
        estado = f"OK  {total:>12,} filas" if ok else "FAIL"
        print(f"  {table:<12}: {estado}")

    ok_count = sum(1 for _, _, ok in results if ok)
    print(f"\nExitosas : {ok_count}/{len(results)}")
    if ok_count == len(results):
        print(f"\nBD BASE lista: {DATABASE_NAME}")
        print("Usa 2_indices.py, 3_compresion.py, 4_indices_compresion.py para optimizaciones.")
    else:
        print(f"Fallidas : {len(results) - ok_count}")

    conn.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.")
    except Exception as e:
        print(f"\n[ERROR] {e}")
        sys.exit(1)
