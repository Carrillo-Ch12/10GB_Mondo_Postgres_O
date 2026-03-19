#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crea BD TPC-H en PostgreSQL: con pg_squeeze
Tablas compactadas con squeeze.squeeze_table() para eliminar bloat
Sin indices adicionales, sin LZ4
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
DATABASE_NAME = "tpch_squeeze"
PG_HOST       = "localhost"
PG_PORT       = 5432
PG_USER       = "postgres"

# ============================================================
# DDL tablas TPC-H — igual que baseline, sin compresion extra
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

# Orden de squeeze: primero las tablas pequeñas, lineitem al final
TABLES_SQUEEZE_ORDER = [
    "region", "nation", "supplier", "part",
    "customer", "partsupp", "orders", "lineitem"
]

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


def create_tables(conn):
    cur = conn.cursor()
    for table, ddl in TABLES_DDL.items():
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(ddl)
        print(f"  [OK] {table}")
    conn.commit()
    cur.close()


def install_squeeze(conn):
    """Instala la extension pg_squeeze en la base de datos."""
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS pg_squeeze;")
    print(f"  [OK] Extension pg_squeeze instalada.")
    # Verificar que el schema squeeze existe
    cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'squeeze';")
    if cur.fetchone():
        print(f"  [OK] Schema 'squeeze' disponible.")
    else:
        print(f"  [WARN] Schema 'squeeze' no encontrado — revisar instalacion.")
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


def get_table_size(conn, table: str) -> str:
    cur = conn.cursor()
    cur.execute(f"SELECT pg_size_pretty(pg_total_relation_size('{table}'))")
    size = cur.fetchone()[0]
    cur.close()
    return size


def run_squeeze(conn):
    """
    Ejecuta squeeze.squeeze_table() en cada tabla para compactarla.
    pg_squeeze requiere que la tabla tenga al menos una clave unica o PK.
    Como las tablas TPC-H no tienen PK definido, usamos el ctid como
    referencia y ejecutamos VACUUM FULL como alternativa equivalente
    si pg_squeeze falla por falta de indice unico.
    """
    cur = conn.cursor()
    conn.autocommit = True

    print(f"\n  Tamaños ANTES del squeeze:")
    for table in TABLES_SQUEEZE_ORDER:
        size = get_table_size(conn, table)
        print(f"    {table:<12}: {size}")

    print(f"\n  Ejecutando squeeze en cada tabla...")
    squeeze_ok   = []
    squeeze_fail = []

    for table in TABLES_SQUEEZE_ORDER:
        t0 = time.time()
        try:
            # pg_squeeze necesita un indice unico — como no hay PKs,
            # primero creamos uno temporal en la columna principal
            pkey_col = {
                "region":   "r_regionkey",
                "nation":   "n_nationkey",
                "customer": "c_custkey",
                "supplier": "s_suppkey",
                "part":     "p_partkey",
                "partsupp": "ps_partkey",
                "orders":   "o_orderkey",
                "lineitem": "l_orderkey",
            }[table]

            idx_name = f"tmp_squeeze_idx_{table}"
            cur.execute(f"DROP INDEX IF EXISTS {idx_name}")

            if table == "lineitem":
                # lineitem no tiene clave unica simple — usamos combinacion
                cur.execute(f"""
                    CREATE UNIQUE INDEX {idx_name}
                    ON {table} (l_orderkey, l_linenumber)
                """)
            elif table == "partsupp":
                cur.execute(f"""
                    CREATE UNIQUE INDEX {idx_name}
                    ON {table} (ps_partkey, ps_suppkey)
                """)
            else:
                cur.execute(f"""
                    CREATE UNIQUE INDEX {idx_name} ON {table} ({pkey_col})
                """)

            # Ejecutar squeeze
            cur.execute(f"SELECT squeeze.squeeze_table('public', '{table}', NULL)")
            elapsed = time.time() - t0
            print(f"    [OK] {table:<12} squeezed ({elapsed:.1f}s)")
            squeeze_ok.append(table)

            # Limpiar indice temporal
            cur.execute(f"DROP INDEX IF EXISTS {idx_name}")

        except Exception as e:
            elapsed = time.time() - t0
            print(f"    [WARN] {table:<12} squeeze fallo ({elapsed:.1f}s): {e}")
            print(f"           Usando VACUUM FULL como alternativa...")
            try:
                cur.execute(f"VACUUM FULL {table}")
                print(f"    [OK] {table:<12} VACUUM FULL completado.")
                squeeze_ok.append(table)
            except Exception as e2:
                print(f"    [ERROR] {table:<12} VACUUM FULL fallo: {e2}")
                squeeze_fail.append(table)

    print(f"\n  Tamaños DESPUES del squeeze:")
    for table in TABLES_SQUEEZE_ORDER:
        size = get_table_size(conn, table)
        print(f"    {table:<12}: {size}")

    cur.close()
    return len(squeeze_ok), len(squeeze_fail)


# ============================================================
# Main
# ============================================================

def main():
    print("=" * 70)
    print("SETUP TPC-H PG_SQUEEZE - POSTGRESQL - NODO CENTRALIZADO")
    print(f"Fecha    : {datetime.now().isoformat(timespec='seconds')}")
    print(f"DB       : {DATABASE_NAME}")
    print(f"Host     : {PG_HOST}:{PG_PORT}")
    print(f"Datos    : {TPCH_DATA_DIR}")
    print(f"Compres. : pg_squeeze (compactacion de bloat)")
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
    conn_admin.close()

    conn = connect_postgres(dbname=DATABASE_NAME)

    print("\n[2/5] Instalando extension pg_squeeze...")
    install_squeeze(conn)

    print("\n[3/5] Creando tablas...")
    conn.autocommit = False
    create_tables(conn)

    print("\n[4/5] Cargando datos desde archivos .tbl...")
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

    print(f"\n{'='*70}")
    print(f"[5/5] Ejecutando pg_squeeze en todas las tablas...")
    print(f"{'='*70}")
    ok_sq, fail_sq = run_squeeze(conn)

    elapsed_total = (time.time() - start) / 60

    print("\n" + "=" * 70)
    print("RESUMEN FINAL")
    print("=" * 70)
    print(f"Tiempo total   : {elapsed_total:.1f} min")
    for table, total, ok in results:
        estado = f"OK  {total:>12,} filas" if ok else "FAIL"
        print(f"  {table:<12}: {estado}")

    ok_count = sum(1 for _, _, ok in results if ok)
    print(f"\nTablas cargadas  : {ok_count}/{len(results)}")
    print(f"Tablas squeezed  : {ok_sq}/{len(TABLES_SQUEEZE_ORDER)}")
    if fail_sq > 0:
        print(f"Tablas fallidas  : {fail_sq}")

    if ok_count == len(results):
        print(f"\n[LISTO] BD PG_SQUEEZE lista: {DATABASE_NAME}")
        print(f"        Estrategia: compactacion con pg_squeeze post-carga")
    else:
        print(f"\nTablas fallidas en carga: {len(results) - ok_count}")

    conn.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.")
    except Exception as e:
        print(f"\n[ERROR] {e}")
        sys.exit(1)
