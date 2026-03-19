#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crea BD base TPC-H en MongoDB: sin optimizaciones
Sin compresion (block_compressor=none), sin indices adicionales (solo _id)
Nodo centralizado: localhost
"""

import os
import sys
import time
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# ============================================================
# CONFIG
# ============================================================

TPCH_DATA_DIR = "/home/ana-aguilera/Rodolfo/datos/tpc-h"
DATABASE_NAME = "tpch_base"
MONGO_HOST    = "localhost"
MONGO_PORT    = 27017
BATCH_SIZE    = 10_000

# ============================================================
# TPC-H tables schema
# ============================================================

TABLES = {
    "region": {
        "fields": ["r_regionkey", "r_name", "r_comment"],
        "types":  ["int",         "str",    "str"],
        "file":   "region.tbl",
    },
    "nation": {
        "fields": ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
        "types":  ["int",         "str",    "int",          "str"],
        "file":   "nation.tbl",
    },
    "customer": {
        "fields": ["c_custkey", "c_name", "c_address", "c_nationkey",
                   "c_phone",   "c_acctbal", "c_mktsegment", "c_comment"],
        "types":  ["int",       "str",    "str",        "int",
                   "str",       "float",  "str",         "str"],
        "file":   "customer.tbl",
    },
    "supplier": {
        "fields": ["s_suppkey", "s_name", "s_address", "s_nationkey",
                   "s_phone",   "s_acctbal", "s_comment"],
        "types":  ["int",       "str",    "str",        "int",
                   "str",       "float",  "str"],
        "file":   "supplier.tbl",
    },
    "part": {
        "fields": ["p_partkey", "p_name", "p_mfgr", "p_brand", "p_type",
                   "p_size",    "p_container", "p_retailprice", "p_comment"],
        "types":  ["int",       "str",    "str",    "str",    "str",
                   "int",       "str",    "float",  "str"],
        "file":   "part.tbl",
    },
    "partsupp": {
        "fields": ["ps_partkey", "ps_suppkey", "ps_availqty",
                   "ps_supplycost", "ps_comment"],
        "types":  ["int",         "int",        "int",
                   "float",       "str"],
        "file":   "partsupp.tbl",
    },
    "orders": {
        "fields": ["o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice",
                   "o_orderdate", "o_orderpriority", "o_clerk",
                   "o_shippriority", "o_comment"],
        "types":  ["int",         "int",       "str",           "float",
                   "date",        "str",        "str",
                   "int",          "str"],
        "file":   "orders.tbl",
    },
    "lineitem": {
        "fields": ["l_orderkey",   "l_partkey",  "l_suppkey",    "l_linenumber",
                   "l_quantity",   "l_extendedprice", "l_discount", "l_tax",
                   "l_returnflag", "l_linestatus","l_shipdate",  "l_commitdate",
                   "l_receiptdate","l_shipinstruct","l_shipmode", "l_comment"],
        "types":  ["int",           "int",         "int",          "int",
                   "float",         "float",        "float",        "float",
                   "str",           "str",          "date",         "date",
                   "date",          "str",          "str",          "str"],
        "file":   "lineitem.tbl",
    },
}

# ============================================================
# Helpers
# ============================================================

def parse_value(val: str, typ: str):
    if typ == "int":
        return int(val)
    elif typ == "float":
        return float(val)
    elif typ == "date":
        return datetime.strptime(val, "%Y-%m-%d")
    else:
        return val


def parse_line(line: str, fields: list, types: list) -> dict:
    parts = line.rstrip("\n").rstrip("|").split("|")
    return {field: parse_value(parts[i], typ)
            for i, (field, typ) in enumerate(zip(fields, types))}


def create_collection_no_compression(db, name: str):
    if name in db.list_collection_names():
        db.drop_collection(name)
    db.create_collection(
        name,
        storageEngine={"wiredTiger": {"configString": "block_compressor=none"}}
    )


def load_table(db, table: str, info: dict) -> int:
    fpath = os.path.join(TPCH_DATA_DIR, info["file"])
    if not os.path.exists(fpath):
        raise FileNotFoundError(f"No existe: {fpath}")

    col    = db[table]
    fields = info["fields"]
    types  = info["types"]
    batch  = []
    total  = 0

    with open(fpath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            batch.append(parse_line(line, fields, types))
            if len(batch) >= BATCH_SIZE:
                col.insert_many(batch, ordered=False)
                total += len(batch)
                batch = []
                print(f"    {total:,} documentos insertados...", end="\r")

    if batch:
        col.insert_many(batch, ordered=False)
        total += len(batch)

    print(f"    {total:,} documentos insertados total    ")
    return total


def verify_table(db, table: str) -> int:
    return db[table].estimated_document_count()


# ============================================================
# Main
# ============================================================

def main():
    print("=" * 70)
    print("SETUP TPC-H BASE - MONGODB - NODO CENTRALIZADO")
    print(f"Fecha    : {datetime.now().isoformat(timespec='seconds')}")
    print(f"DB       : {DATABASE_NAME}")
    print(f"Host     : {MONGO_HOST}:{MONGO_PORT}")
    print(f"Datos    : {TPCH_DATA_DIR}")
    print(f"Compres. : NO (block_compressor=none)")
    print(f"Indices  : NO (solo _id)")
    print(f"Batch    : {BATCH_SIZE:,} docs")
    print("=" * 70)

    try:
        client = MongoClient(MONGO_HOST, MONGO_PORT, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        print(f"[OK] Conectado a MongoDB {MONGO_HOST}:{MONGO_PORT}")
    except ConnectionFailure as e:
        print(f"[ERROR] No se pudo conectar a MongoDB: {e}")
        sys.exit(1)

    db = client[DATABASE_NAME]

    print("\n[1/2] Creando colecciones sin compresion...")
    for table in TABLES:
        create_collection_no_compression(db, table)
        print(f"  [OK] {table}")

    print("\n[2/2] Cargando datos desde archivos .tbl...")
    results = []
    start   = time.time()

    for table, info in TABLES.items():
        print(f"\n{'='*70}")
        print(f"[CARGA] {table.upper()}")
        print(f"{'='*70}")
        t0 = time.time()
        try:
            total    = load_table(db, table, info)
            elapsed  = time.time() - t0
            verified = verify_table(db, table)
            print(f"  [OK] {verified:,} documentos en coleccion ({elapsed:.1f}s)")
            results.append((table, total, True))
        except Exception as e:
            print(f"  [ERROR] {e}")
            results.append((table, 0, False))

    elapsed_total = (time.time() - start) / 60

    print("\n" + "=" * 70)
    print("RESUMEN FINAL")
    print("=" * 70)
    print(f"Tiempo total : {elapsed_total:.1f} min")
    for table, total, ok in results:
        estado = f"OK  {total:>12,} docs" if ok else "FAIL"
        print(f"  {table:<12}: {estado}")

    ok_count = sum(1 for _, _, ok in results if ok)
    print(f"\nExitosas : {ok_count}/{len(results)}")
    if ok_count == len(results):
        print(f"\nBD BASE lista: {DATABASE_NAME}")
        print("Usa 2_indices_mongo.py, 3_compresion_mongo.py, 4_indices_compresion_mongo.py para optimizaciones.")
    else:
        print(f"Fallidas : {len(results) - ok_count}")

    client.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.")
    except Exception as e:
        print(f"\n[ERROR] {e}")
        sys.exit(1)
