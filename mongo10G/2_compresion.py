#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crea BD TPC-H en MongoDB con compresion Snappy (sin indices adicionales)
Lee desde tpch_base y escribe en tpch_compresion
Nodo centralizado: localhost
"""

import sys
import time
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# ============================================================
# CONFIG
# ============================================================

SOURCE_DB     = "tpch_base"
DATABASE_NAME = "tpch_compresion"
MONGO_HOST    = "localhost"
MONGO_PORT    = 27017
BATCH_SIZE    = 10_000
COMPRESSION   = "snappy"

# ============================================================
# Tablas TPC-H
# ============================================================

TABLES = [
    "region",
    "nation",
    "customer",
    "supplier",
    "part",
    "partsupp",
    "orders",
    "lineitem",
]

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

    cursor = src_col.find({}, {"_id": 0})
    for doc in cursor:
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


def verify_table(db, table: str) -> int:
    return db[table].estimated_document_count()


# ============================================================
# Main
# ============================================================

def main():
    print("=" * 70)
    print("SETUP TPC-H COMPRESION - MONGODB - NODO CENTRALIZADO")
    print(f"Fecha    : {datetime.now().isoformat(timespec='seconds')}")
    print(f"Fuente   : {SOURCE_DB}")
    print(f"Destino  : {DATABASE_NAME}")
    print(f"Host     : {MONGO_HOST}:{MONGO_PORT}")
    print(f"Compres. : {COMPRESSION.upper()} (block_compressor={COMPRESSION})")
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

    src_db = client[SOURCE_DB]
    dst_db = client[DATABASE_NAME]

    # Verificar que SOURCE_DB existe y tiene tablas
    src_tables = src_db.list_collection_names()
    if not src_tables:
        print(f"\n[ERROR] La base {SOURCE_DB} no existe o no tiene colecciones.")
        print("Ejecuta primero 1_base_mongo.py")
        client.close()
        sys.exit(1)

    missing = [t for t in TABLES if t not in src_tables]
    if missing:
        print(f"\n[ERROR] Faltan colecciones en {SOURCE_DB}: {missing}")
        client.close()
        sys.exit(1)

    print(f"\n[OK] {SOURCE_DB} tiene {len(src_tables)} colecciones disponibles")
    print(f"\n{len(TABLES)} tablas a procesar\n")
    for t in TABLES:
        count = src_db[t].estimated_document_count()
        print(f"  {t:<12}: {count:>12,} docs en fuente")

    print("\n" + "=" * 70)
    resp = input("Continuar? (SI): ")
    if resp.strip().upper() != "SI":
        print("Cancelado.")
        client.close()
        sys.exit(0)

    print("\n[1/2] Creando colecciones con compresion Snappy...")
    for table in TABLES:
        create_collection_snappy(dst_db, table)
        print(f"  [OK] {table}")

    print("\n[2/2] Copiando datos desde tpch_base...")
    results = []
    start   = time.time()

    for i, table in enumerate(TABLES, 1):
        print(f"\n{'='*70}")
        print(f"[{i}/{len(TABLES)}] {table.upper()}")
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
        print(f"\nBD COMPRESION lista: {DATABASE_NAME}")
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
