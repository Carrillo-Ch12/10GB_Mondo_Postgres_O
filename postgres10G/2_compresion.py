#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crea BD TPC-H en PostgreSQL: con compresion LZ4 real (STORAGE MAIN)
Compresion LZ4 inline en todas las columnas TEXT via STORAGE MAIN
Sin indices adicionales (solo PKs)
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
DATABASE_NAME = "tpch_compresion"
PG_HOST       = "localhost"
PG_PORT       = 5432
PG_USER       = "postgres"

# ============================================================
# DDL tablas TPC-H con COMPRESSION lz4 en columnas TEXT
# ============================================================

TABLES_DDL = {
    "region": """
        CREATE TABLE IF NOT EXISTS region (
            r_regionkey  INTEGER,
            r_name       TEXT COMPRESSION lz4,
            r_comment    TEXT COMPRESSION lz4
        )
    """,
    "nation": """
        CREATE TABLE IF NOT EXISTS nation (
            n_nationkey  INTEGER,
            n_name       TEXT COMPRESSION lz4,
            n_regionkey  INTEGER,
            n_comment    TEXT COMPRESSION lz4
        )
    """,
    "customer": """
        CREATE TABLE IF NOT EXISTS customer (
            c_custkey    INTEGER,
            c_name       TEXT COMPRESSION lz4,
            c_address    TEXT COMPRESSION lz4,
            c_nationkey  INTEGER,
            c_phone      TEXT COMPRESSION lz4,
            c_acctbal    DOUBLE PRECISION,
            c_mktsegment TEXT COMPRESSION lz4,
            c_comment    TEXT COMPRESSION lz4
        )
    """,
    "supplier": """
        CREATE TABLE IF NOT EXISTS supplier (
            s_suppkey    INTEGER,
            s_name       TEXT COMPRESSION lz4,
            s_address    TEXT COMPRESSION lz4,
            s_nationkey  INTEGER,
            s_phone      TEXT COMPRESSION lz4,
            s_acctbal    DOUBLE PRECISION,
            s_comment    TEXT COMPRESSION lz4
        )
    """,
    "part": """
        CREATE TABLE IF NOT EXISTS part (
            p_partkey     INTEGER,
            p_name        TEXT COMPRESSION lz4,
            p_mfgr        TEXT COMPRESSION lz4,
            p_brand       TEXT COMPRESSION lz4,
            p_type        TEXT COMPRESSION lz4,
            p_size        INTEGER,
            p_container   TEXT COMPRESSION lz4,
            p_retailprice DOUBLE PRECISION,
            p_comment     TEXT COMPRESSION lz4
        )
    """,
    "partsupp": """
        CREATE TABLE IF NOT EXISTS partsupp (
            ps_partkey    INTEGER,
            ps_suppkey    INTEGER,
            ps_availqty   INTEGER,
            ps_supplycost DOUBLE PRECISION,
            ps_comment    TEXT COMPRESSION lz4
        )
    """,
    "orders": """
        CREATE TABLE IF NOT EXISTS orders (
            o_orderkey      INTEGER,
            o_custkey       INTEGER,
            o_orderstatus   TEXT COMPRESSION lz4,
            o_totalprice    DOUBLE PRECISION,
            o_orderdate     DATE,
            o_orderpriority TEXT COMPRESSION lz4,
            o_clerk         TEXT COMPRESSION lz4,
            o_shippriority  INTEGER,
            o_comment       TEXT COMPRESSION lz4
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
            l_returnflag    TEXT COMPRESSION lz4,
            l_linestatus    TEXT COMPRESSION lz4,
            l_shipdate      DATE,
            l_commitdate    DATE,
            l_receiptdate   DATE,
            l_shipinstruct  TEXT COMPRESSION lz4,
            l_shipmode      TEXT COMPRESSION lz4,
            l_comment       TEXT COMPRESSION lz4
        )
    """,
}

# Columnas TEXT por tabla para aplicar STORAGE MAIN
TABLES_TEXT_COLUMNS = {
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


def drop_and_create_database(conn):
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DATABASE_NAME,))
    if cur.fetchone():
        print(f"  [INFO] Base {DATABASE_NAME} existe, eliminando...")
        # Desconectar sesiones activas antes de borrar
        cur.execute("""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = %s AND pid <> pg_backend_pid()
        """, (DATABASE_NAME,))
        cur.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(DATABASE_NAME)))
        print(f"  [OK] Base {DATABASE_NAME} eliminada.")
    cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DATABASE_NAME)))
    print(f"  [OK] Base de datos {DATABASE_NAME} creada.")
    cur.close()


def set_lz4_default(conn):
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        sql.SQL("ALTER DATABASE {} SET default_toast_compression = 'lz4'")
        .format(sql.Identifier(DATABASE_NAME))
    )
    print(f"  [OK] default_toast_compression = lz4 aplicado a {DATABASE_NAME}")
    cur.close()


def create_tables(conn):
    cur = conn.cursor()
    for table, ddl in TABLES_DDL.items():
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(ddl)
        # Forzar STORAGE MAIN en todas las columnas TEXT para compresion inline real
        for col in TABLES_TEXT_COLUMNS.get(table, []):
            cur.execute(f"ALTER TABLE {table} ALTER COLUMN {col} SET STORAGE main")
        print(f"  [OK] {table} (COMPRESSION lz4 + STORAGE MAIN en columnas TEXT)")
    conn.commit()
    cur.close()


def verify_storage(conn):
    """Verifica que STORAGE MAIN y LZ4 esten aplicados correctamente."""
    cur = conn.cursor()
    cur.execute("""
        SELECT attrelid::regclass AS tabla,
               attname            AS columna,
               attstorage,
               attcompression
        FROM   pg_attribute
        WHERE  attrelid IN (
                   SELECT oid FROM pg_class
                   WHERE  relnamespace = 'public'::regnamespace
                   AND    relkind = 'r'
               )
        AND    attnum > 0
        AND    NOT attisdropped
        AND    attstorage = 'm'   -- solo columnas con STORAGE MAIN
        ORDER  BY tabla, attnum
    """)
    rows = cur.fetchall()
    cur.close()
    return rows


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
    print("SETUP TPC-H COMPRESION REAL - POSTGRESQL - NODO CENTRALIZADO")
    print(f"Fecha    : {datetime.now().isoformat(timespec='seconds')}")
    print(f"DB       : {DATABASE_NAME}")
    print(f"Host     : {PG_HOST}:{PG_PORT}")
    print(f"Datos    : {TPCH_DATA_DIR}")
    print(f"Compres. : LZ4 inline (COMPRESSION lz4 + STORAGE MAIN en TEXT)")
    print(f"Indices  : NO (sin indices adicionales)")
    print("=" * 70)

    try:
        conn_admin = connect_postgres(dbname="postgres")
        print(f"[OK] Conectado a PostgreSQL {PG_HOST}:{PG_PORT}")
    except Exception as e:
        print(f"[ERROR] No se pudo conectar a PostgreSQL: {e}")
        sys.exit(1)

    print("\n[1/5] Eliminando (si existe) y creando base de datos...")
    drop_and_create_database(conn_admin)

    print("\n[2/5] Configurando LZ4 como compresion por defecto...")
    set_lz4_default(conn_admin)
    conn_admin.close()

    conn = connect_postgres(dbname=DATABASE_NAME)

    print("\n[3/5] Creando tablas con COMPRESSION lz4 + STORAGE MAIN...")
    create_tables(conn)

    print("\n[4/5] Verificando configuracion de almacenamiento...")
    rows = verify_storage(conn)
    if rows:
        print(f"  [OK] {len(rows)} columnas con STORAGE MAIN + LZ4 confirmadas:")
        for tabla, col, storage, comp in rows:
            print(f"       {tabla}.{col} → storage={storage}, compression={comp}")
    else:
        print("  [WARN] No se encontraron columnas con STORAGE MAIN — revisar DDL")

    print("\n[5/5] Cargando datos desde archivos .tbl...")
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
        print(f"\n[LISTO] BD COMPRESION REAL lista: {DATABASE_NAME}")
        print(f"        Estrategia: COMPRESSION lz4 + STORAGE MAIN en columnas TEXT")
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
