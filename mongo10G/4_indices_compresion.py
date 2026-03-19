#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crea BD TPC-H en MongoDB con compresion Snappy + indices especificos por query
Lee desde tpch_base y escribe en tpch_indices_compresion
Nodo centralizado: localhost
"""

import sys
import time
from datetime import datetime
from pymongo import MongoClient, ASCENDING, TEXT
from pymongo.errors import ConnectionFailure

# ============================================================
# CONFIG
# ============================================================

SOURCE_DB     = "tpch_base"
DATABASE_NAME = "tpch_indices_compresion"
MONGO_HOST    = "localhost"
MONGO_PORT    = 27017
BATCH_SIZE    = 10_000
COMPRESSION   = "snappy"

# ============================================================
# Indices por coleccion
# INCLUDE en SQL -> campos regulares del indice en MongoDB
# text_pattern_ops -> TEXT index en MongoDB
# ============================================================

INDICES = {
    "lineitem": [
        {"name": "q01i1", "keys": [("l_shipdate", ASCENDING), ("l_returnflag", ASCENDING), ("l_linestatus", ASCENDING)]},
        {"name": "q03i1", "keys": [("l_shipdate", ASCENDING), ("l_orderkey", ASCENDING)]},
        {"name": "q04i2", "keys": [("l_orderkey", ASCENDING), ("l_commitdate", ASCENDING), ("l_receiptdate", ASCENDING)]},
        {"name": "q05i2", "keys": [("l_orderkey", ASCENDING), ("l_suppkey", ASCENDING), ("l_extendedprice", ASCENDING), ("l_discount", ASCENDING)]},
        {"name": "q06i1", "keys": [("l_shipdate", ASCENDING), ("l_discount", ASCENDING), ("l_quantity", ASCENDING), ("l_extendedprice", ASCENDING)]},
        {"name": "q07i1", "keys": [("l_shipdate", ASCENDING), ("l_suppkey", ASCENDING), ("l_orderkey", ASCENDING), ("l_extendedprice", ASCENDING), ("l_discount", ASCENDING)]},
        {"name": "q08i1", "keys": [("l_shipdate", ASCENDING), ("l_orderkey", ASCENDING), ("l_suppkey", ASCENDING)]},
        {"name": "q08i2", "keys": [("l_partkey", ASCENDING), ("l_orderkey", ASCENDING), ("l_suppkey", ASCENDING)]},
        {"name": "q09i2", "keys": [("l_partkey", ASCENDING), ("l_suppkey", ASCENDING), ("l_orderkey", ASCENDING)]},
        {"name": "q10i1", "keys": [("l_returnflag", ASCENDING), ("l_orderkey", ASCENDING), ("l_extendedprice", ASCENDING), ("l_discount", ASCENDING)]},
        {"name": "q12i2", "keys": [("l_receiptdate", ASCENDING), ("l_orderkey", ASCENDING), ("l_shipmode", ASCENDING), ("l_commitdate", ASCENDING), ("l_shipdate", ASCENDING)]},
        {"name": "q14i1", "keys": [("l_shipdate", ASCENDING), ("l_partkey", ASCENDING), ("l_extendedprice", ASCENDING), ("l_discount", ASCENDING)]},
        {"name": "q18i1", "keys": [("l_orderkey", ASCENDING), ("l_quantity", ASCENDING)]},
        {"name": "q21i1", "keys": [("l_orderkey", ASCENDING), ("l_suppkey", ASCENDING), ("l_receiptdate", ASCENDING), ("l_commitdate", ASCENDING)]},
    ],
    "part": [
        {"name": "q02i1", "keys": [("p_type", ASCENDING), ("p_size", ASCENDING), ("p_partkey", ASCENDING)]},
        {"name": "q09i1", "keys": [("p_name", ASCENDING), ("p_partkey", ASCENDING)]},
        {"name": "q16i1", "keys": [("p_partkey", ASCENDING), ("p_brand", ASCENDING), ("p_type", ASCENDING), ("p_size", ASCENDING)]},
        {"name": "q17i1", "keys": [("p_partkey", ASCENDING), ("p_brand", ASCENDING), ("p_container", ASCENDING)]},
        {"name": "q19i1", "keys": [("p_brand", ASCENDING), ("p_container", ASCENDING), ("p_size", ASCENDING), ("p_partkey", ASCENDING)]},
        {"name": "q20i3", "keys": [("p_name", TEXT)]},
    ],
    "region": [
        {"name": "q02i2", "keys": [("r_name", ASCENDING), ("r_regionkey", ASCENDING)]},
        {"name": "q05i6", "keys": [("r_regionkey", ASCENDING), ("r_name", ASCENDING)]},
    ],
    "nation": [
        {"name": "q02i3", "keys": [("n_regionkey", ASCENDING), ("n_nationkey", ASCENDING)]},
        {"name": "q05i5", "keys": [("n_nationkey", ASCENDING), ("n_regionkey", ASCENDING), ("n_name", ASCENDING)]},
        {"name": "q11i2", "keys": [("n_name", ASCENDING), ("n_nationkey", ASCENDING)]},
    ],
    "supplier": [
        {"name": "q02i4", "keys": [("s_nationkey", ASCENDING), ("s_suppkey", ASCENDING)]},
        {"name": "q05i4", "keys": [("s_suppkey", ASCENDING), ("s_nationkey", ASCENDING)]},
        {"name": "q16i2", "keys": [("s_suppkey", ASCENDING), ("s_comment", ASCENDING)]},
    ],
    "partsupp": [
        {"name": "q02i5", "keys": [("ps_partkey", ASCENDING), ("ps_supplycost", ASCENDING), ("ps_suppkey", ASCENDING)]},
        {"name": "q09i3", "keys": [("ps_partkey", ASCENDING), ("ps_suppkey", ASCENDING), ("ps_supplycost", ASCENDING)]},
        {"name": "q11i1", "keys": [("ps_suppkey", ASCENDING), ("ps_partkey", ASCENDING), ("ps_supplycost", ASCENDING), ("ps_availqty", ASCENDING)]},
        {"name": "q20i2", "keys": [("ps_partkey", ASCENDING), ("ps_suppkey", ASCENDING), ("ps_availqty", ASCENDING)]},
    ],
    "orders": [
        {"name": "q03i2", "keys": [("o_orderdate", ASCENDING), ("o_custkey", ASCENDING), ("o_orderkey", ASCENDING), ("o_shippriority", ASCENDING)]},
        {"name": "q04i1", "keys": [("o_orderdate", ASCENDING), ("o_orderkey", ASCENDING), ("o_orderpriority", ASCENDING)]},
        {"name": "q05i1", "keys": [("o_orderdate", ASCENDING), ("o_orderkey", ASCENDING), ("o_custkey", ASCENDING)]},
        {"name": "q07i2", "keys": [("o_orderkey", ASCENDING), ("o_custkey", ASCENDING)]},
        {"name": "q12i1", "keys": [("o_orderkey", ASCENDING), ("o_orderpriority", ASCENDING)]},
        {"name": "q21i2", "keys": [("o_orderkey", ASCENDING), ("o_orderstatus", ASCENDING)]},
    ],
    "customer": [
        {"name": "q03i3", "keys": [("c_mktsegment", ASCENDING), ("c_custkey", ASCENDING)]},
        {"name": "q05i3", "keys": [("c_custkey", ASCENDING), ("c_nationkey", ASCENDING)]},
        {"name": "q05i7", "keys": [("c_nationkey", ASCENDING), ("c_custkey", ASCENDING)]},
        {"name": "q18i3", "keys": [("c_custkey", ASCENDING), ("c_name", ASCENDING)]},
        {"name": "q22i1", "keys": [("c_phone", ASCENDING)]},
    ],
}

TABLES = ["region", "nation", "customer", "supplier",
          "part", "partsupp", "orders", "lineitem"]

# ============================================================
# Helpers
# ============================================================

def create_collection_snappy(db, name: str):
    if name in db.list_collection_names():
        db.drop_collection(name)
    db.create_collection(
        name,
        storageEngine={"wiredTiger": {"configString": f"block_compressor={COMPRESSION}"}}
    )


def copy_table(src_db, dst_db, table: str) -> int:
    src_col = src_db[table]
    dst_col = dst_db[table]
    total   = 0
    batch   = []

    for doc in src_col.find({}, {"_id": 0}):
        batch.append(doc)
        if len(batch) >= BATCH_SIZE:
            dst_col.insert_many(batch, ordered=False)
            total += len(batch)
            batch = []
            print(f"    {total:,} documentos copiados...", end="\r")

    if batch:
        dst_col.insert_many(batch, ordered=False)
        total += len(batch)

    print(f"    {total:,} documentos copiados total    ")
    return total


def create_indices(db, table: str) -> int:
    col        = db[table]
    index_list = INDICES.get(table, [])
    created    = 0
    for idx in index_list:
        try:
            col.create_index(idx["keys"], name=idx["name"])
            print(f"    [OK] {idx['name']}: {[k[0] for k in idx['keys']]}")
            created += 1
        except Exception as e:
            print(f"    [ERROR] {idx['name']}: {e}")
    return created


def verify_table(db, table: str) -> int:
    return db[table].estimated_document_count()


# ============================================================
# Main
# ============================================================

def main():
    total_indices = sum(len(v) for v in INDICES.values())

    print("=" * 70)
    print("SETUP TPC-H INDICES + COMPRESION - MONGODB - NODO CENTRALIZADO")
    print(f"Fecha    : {datetime.now().isoformat(timespec='seconds')}")
    print(f"Fuente   : {SOURCE_DB}")
    print(f"Destino  : {DATABASE_NAME}")
    print(f"Host     : {MONGO_HOST}:{MONGO_PORT}")
    print(f"Compres. : {COMPRESSION.upper()} (block_compressor={COMPRESSION})")
    print(f"Indices  : {total_indices} indices en {len(INDICES)} colecciones")
    print("=" * 70)

    try:
        client = MongoClient(MONGO_HOST, MONGO_PORT, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        print(f"[OK] Conectado a MongoDB {MONGO_HOST}:{MONGO_PORT}")
    except ConnectionFailure as e:
        print(f"[ERROR] No se pudo conectar a MongoDB: {e}")
        sys.exit(1)

    src_db = client[SOURCE_DB]
    dst_db = client[DATABASE_NAME]

    src_tables = src_db.list_collection_names()
    if not src_tables:
        print(f"\n[ERROR] La base {SOURCE_DB} no existe o no tiene colecciones.")
        print("Ejecuta primero 1_baseline.py")
        client.close()
        sys.exit(1)

    missing = [t for t in TABLES if t not in src_tables]
    if missing:
        print(f"\n[ERROR] Faltan colecciones en {SOURCE_DB}: {missing}")
        client.close()
        sys.exit(1)

    print(f"\n[OK] {SOURCE_DB} tiene {len(src_tables)} colecciones disponibles")
    print(f"\n{len(TABLES)} tablas | {total_indices} indices totales\n")
    for t in TABLES:
        count = src_db[t].estimated_document_count()
        n_idx = len(INDICES.get(t, []))
        print(f"  {t:<12}: {count:>12,} docs | {n_idx} indices")

    print("\n" + "=" * 70)
    resp = input("Continuar? (SI): ")
    if resp.strip().upper() != "SI":
        print("Cancelado.")
        client.close()
        sys.exit(0)

    print("\n[1/3] Creando colecciones con compresion Snappy...")
    for table in TABLES:
        create_collection_snappy(dst_db, table)
        print(f"  [OK] {table}")

    print("\n[2/3] Copiando datos desde tpch_base...")
    results = []
    start   = time.time()

    for i, table in enumerate(TABLES, 1):
        print(f"\n{'='*70}")
        print(f"[{i}/{len(TABLES)}] {table.upper()} - COPIA")
        print(f"{'='*70}")
        t0 = time.time()
        try:
            total    = copy_table(src_db, dst_db, table)
            elapsed  = time.time() - t0
            verified = verify_table(dst_db, table)
            print(f"  [OK] {verified:,} documentos en coleccion ({elapsed:.1f}s)")
            results.append((table, total, True))
        except Exception as e:
            print(f"  [ERROR] {e}")
            results.append((table, 0, False))

    print("\n[3/3] Creando indices...")
    idx_results = []
    for i, table in enumerate(TABLES, 1):
        print(f"\n{'='*70}")
        print(f"[{i}/{len(TABLES)}] {table.upper()} - INDICES")
        print(f"{'='*70}")
        n_idx   = len(INDICES.get(table, []))
        created = create_indices(dst_db, table)
        idx_results.append((table, created, n_idx))

    elapsed_total = (time.time() - start) / 60

    print("\n" + "=" * 70)
    print("RESUMEN FINAL")
    print("=" * 70)
    print(f"Tiempo total : {elapsed_total:.1f} min")
    print(f"\n{'Coleccion':<12} {'Docs':>12}  {'Indices':>10}")
    print("-" * 40)
    for (table, total, ok), (_, created, n_idx) in zip(results, idx_results):
        estado_docs = f"{total:>12,}" if ok else "FAIL"
        print(f"  {table:<12} {estado_docs}  {created}/{n_idx} indices")

    ok_count = sum(1 for _, _, ok in results if ok)
    print(f"\nTablas cargadas : {ok_count}/{len(results)}")
    print(f"Indices creados : {sum(c for _, c, _ in idx_results)}/{total_indices}")
    if ok_count == len(results):
        print(f"\nBD INDICES + COMPRESION lista: {DATABASE_NAME}")
    client.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.")
    except Exception as e:
        print(f"\n[ERROR] {e}")
        sys.exit(1)
