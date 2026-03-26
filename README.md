# Experimento TPC-H 10GB — MongoDB y PostgreSQL

Comparación de diseños de bases de datos (baseline, compresión, índices, índices+compresión)
usando el benchmark TPC-H con ~10GB de datos, sobre discos virtuales dedicados.

---

## Infraestructura

### Hardware
| Componente | Detalle |
|---|---|
| Máquina | ThinkStation P720 |
| SO | Ubuntu 24.04 |
| Disco principal | NVMe 468GB (`/dev/nvme1n1p2`) |
| Disco secundario | HDD 5.5TB (`/dev/sda2`) |
| Disco Windows | NVMe 475GB (`/dev/nvme0n1p3`) |

### Discos virtuales (loop devices)
Las bases de datos se almacenaron en discos virtuales dedicados creados sobre el disco Windows,
para aislar el almacenamiento del sistema operativo:

| Motor | Loop device | Punto de montaje | Tamaño |
|---|---|---|---|
| MongoDB | `/dev/loop28` | `/mnt/mongodb-disk` | 147GB |
| PostgreSQL | `/dev/loop29` | `/mnt/postgres-disk` | 196GB |

---

## Paso 1 — Creación de discos virtuales (Loop Devices)

Se creó un archivo de imagen para cada motor y se montó como disco independiente.
El proceso fue idéntico para MongoDB y PostgreSQL, cambiando rutas y nombres.

### MongoDB

```bash
# Crear imagen de disco de 150GB
sudo dd if=/dev/zero of=/media/ana-aguilera/Windows/mongodb.img bs=1G count=150 status=progress

# Formatear como ext4
sudo mkfs.ext4 /media/ana-aguilera/Windows/mongodb.img

# Crear punto de montaje y montar
sudo mkdir -p /mnt/mongodb-disk
sudo mount -o loop /media/ana-aguilera/Windows/mongodb.img /mnt/mongodb-disk

# Crear carpeta de datos y asignar permisos a MongoDB
sudo mkdir -p /mnt/mongodb-disk/data
sudo chown -R mongodb:mongodb /mnt/mongodb-disk/data
sudo chmod 755 /mnt/mongodb-disk/data
```

Editar `/etc/mongod.conf` para apuntar al nuevo disco:
```yaml
storage:
  dbPath: /mnt/mongodb-disk/data
```

Permitir acceso al path desde systemd:
```bash
sudo systemctl edit mongod
```
```ini
[Service]
ReadWritePaths=/mnt/mongodb-disk/data
```

```bash
sudo systemctl daemon-reload
sudo systemctl start mongod
sudo systemctl status mongod

# Verificar que MongoDB usa el nuevo path
mongosh --eval "db.adminCommand({getCmdLineOpts: 1})" | grep dbPath
```

### PostgreSQL

A diferencia de MongoDB, el disco virtual de PostgreSQL se creó sobre el **HDD externo de 6TB**
(`/dev/sda2`, montado en `/media/ana-aguilera/HDD6TB`) en vez del disco Windows:

```bash
# Crear imagen de disco de 200GB en el HDD externo
sudo dd if=/dev/zero of=/media/ana-aguilera/HDD6TB/postgres.img bs=1G count=200 status=progress

# Formatear como ext4
sudo mkfs.ext4 /media/ana-aguilera/HDD6TB/postgres.img

# Crear punto de montaje y montar
sudo mkdir -p /mnt/postgres-disk
sudo mount -o loop /media/ana-aguilera/HDD6TB/postgres.img /mnt/postgres-disk

# Crear carpeta de datos y asignar permisos a PostgreSQL
sudo mkdir -p /mnt/postgres-disk/data
sudo chown -R postgres:postgres /mnt/postgres-disk/data
sudo chmod 755 /mnt/postgres-disk/data
```

Luego se configuró PostgreSQL para usar ese directorio como `data_directory` y se reinició el servicio.

---

## Paso 2 — Carga de datos TPC-H

Los datos fueron generados con el benchmark TPC-H (~10GB) y cargados mediante scripts Python
en cada base de datos. Los archivos `.tbl` se encontraban en:
```
/home/ana-aguilera/Rodolfo/datos/tpc-h/
```

Las tablas del esquema TPC-H son:

| Tabla | Filas aprox. |
|---|---|
| `lineitem` | 59,986,052 |
| `orders` | 15,000,000 |
| `partsupp` | 8,000,000 |
| `part` | 2,000,000 |
| `customer` | 1,500,000 |
| `supplier` | 100,000 |
| `nation` | 25 |
| `region` | 5 |

---

## Paso 3 — Diseños de bases de datos

Se crearon 4 diseños distintos para comparar rendimiento, más una quinta base en PostgreSQL
a pedido de la profesora usando `pg_squeeze`.

### Índices utilizados (comunes a MongoDB y PostgreSQL)

```sql
CREATE INDEX q01i1 ON lineitem  (l_shipdate, l_returnflag, l_linestatus);
CREATE INDEX q02i1 ON part      (p_type, p_size, p_partkey);
CREATE INDEX q02i2 ON region    (r_name, r_regionkey);
CREATE INDEX q02i3 ON nation    (n_regionkey, n_nationkey);
CREATE INDEX q02i4 ON supplier  (s_nationkey, s_suppkey);
CREATE INDEX q02i5 ON partsupp  (ps_partkey, ps_supplycost, ps_suppkey);
CREATE INDEX q03i1 ON lineitem  (l_shipdate, l_orderkey);
CREATE INDEX q03i2 ON orders    (o_orderdate, o_custkey, o_orderkey) INCLUDE (o_shippriority);
CREATE INDEX q03i3 ON customer  (c_mktsegment, c_custkey);
CREATE INDEX q04i1 ON orders    (o_orderdate, o_orderkey, o_orderpriority);
CREATE INDEX q04i2 ON lineitem  (l_orderkey, l_commitdate, l_receiptdate);
CREATE INDEX q05i1 ON orders    (o_orderdate, o_orderkey, o_custkey);
CREATE INDEX q05i2 ON lineitem  (l_orderkey, l_suppkey, l_extendedprice, l_discount);
CREATE INDEX q05i3 ON customer  (c_custkey, c_nationkey);
CREATE INDEX q05i4 ON supplier  (s_suppkey, s_nationkey);
CREATE INDEX q05i5 ON nation    (n_nationkey, n_regionkey, n_name);
CREATE INDEX q05i6 ON region    (r_regionkey, r_name);
CREATE INDEX q05i7 ON customer  (c_nationkey, c_custkey);
CREATE INDEX q06i1 ON lineitem  (l_shipdate, l_discount, l_quantity) INCLUDE (l_extendedprice);
CREATE INDEX q07i1 ON lineitem  (l_shipdate, l_suppkey, l_orderkey) INCLUDE (l_extendedprice, l_discount);
CREATE INDEX q07i2 ON orders    (o_orderkey, o_custkey);
CREATE INDEX q08i1 ON lineitem  (l_shipdate, l_orderkey, l_suppkey);
CREATE INDEX q08i2 ON lineitem  (l_partkey, l_orderkey, l_suppkey);
CREATE INDEX q09i1 ON part      (p_name, p_partkey);
CREATE INDEX q09i2 ON lineitem  (l_partkey, l_suppkey, l_orderkey);
CREATE INDEX q09i3 ON partsupp  (ps_partkey, ps_suppkey, ps_supplycost);
CREATE INDEX q10i1 ON lineitem  (l_returnflag, l_orderkey, l_extendedprice, l_discount);
CREATE INDEX q11i1 ON partsupp  (ps_suppkey, ps_partkey, ps_supplycost, ps_availqty);
CREATE INDEX q11i2 ON nation    (n_name, n_nationkey);
CREATE INDEX q12i1 ON orders    (o_orderkey, o_orderpriority);
CREATE INDEX q12i2 ON lineitem  (l_receiptdate, l_orderkey) INCLUDE (l_shipmode, l_commitdate, l_shipdate);
CREATE INDEX q14i1 ON lineitem  (l_shipdate, l_partkey) INCLUDE (l_extendedprice, l_discount);
CREATE INDEX q16i1 ON part      (p_partkey, p_brand, p_type, p_size);
CREATE INDEX q16i2 ON supplier  (s_suppkey, s_comment);
CREATE INDEX q17i1 ON part      (p_partkey, p_brand, p_container);
CREATE INDEX q18i1 ON lineitem  (l_orderkey, l_quantity);
CREATE INDEX q18i3 ON customer  (c_custkey, c_name);
CREATE INDEX q19i1 ON part      (p_brand, p_container, p_size, p_partkey);
CREATE INDEX q20i2 ON partsupp  (ps_partkey, ps_suppkey, ps_availqty);
CREATE INDEX q20i3 ON part      (p_name text_pattern_ops);
CREATE INDEX q21i1 ON lineitem  (l_orderkey, l_suppkey, l_receiptdate, l_commitdate);
CREATE INDEX q21i2 ON orders    (o_orderkey, o_orderstatus);
CREATE INDEX q22i1 ON customer  (c_phone);
```

---

## MongoDB

### Scripts

| Script | Base de datos | Descripción |
|---|---|---|
| `1_baseline.py` | `tpch_base` | Sin índices, sin compresión |
| `2_compresion.py` | `tpch_compresion` | Compresión Snappy en todas las colecciones |
| `3_indices.py` | `tpch_indices` | 43 índices, sin compresión |
| `4_indices_compresion.py` | `tpch_indices_compresion` | 43 índices + compresión Snappy |

### Compresión utilizada
MongoDB utiliza **Snappy** como compresor de bloques WiredTiger, configurado a nivel de colección.
Snappy es efectivo en MongoDB porque almacena documentos BSON completos, que son suficientemente
grandes para comprimir.

### Resultados de tamaño

| Base de datos | Tamaño |
|---|---|
| `tpch_base` | 28.68 GiB |
| `tpch_compresion` | 10.90 GiB |
| `tpch_indices` | 50.00 GiB |
| `tpch_indices_compresion` | 32.28 GiB |

**Ahorro de compresión Snappy: ~62% respecto a baseline.**

---

## PostgreSQL

### Scripts

| Script | Base de datos | Descripción |
|---|---|---|
| `1_base.py` | `tpch_base` | Sin índices, sin compresión |
| `2_compresion.py` | `tpch_compresion` | Compresión LZ4 + STORAGE MAIN en columnas TEXT |
| `3_indices.py` | `tpch_indices` | 43 índices, sin compresión |
| `4_indices_compresion.py` | `tpch_indices_compresion` | 43 índices + compresión LZ4 |
| `5_compresion_pg_squeeze.py` | `tpch_squeeze` | pg_squeeze (a pedido de la profesora) |

### Compresión utilizada
PostgreSQL utiliza **LZ4** con `COMPRESSION lz4` + `STORAGE MAIN` en columnas TEXT,
forzando compresión inline independiente del tamaño del valor. Adicionalmente se instaló
y configuró la extensión `pg_squeeze` para una quinta base de datos experimental.

### Extensión pg_squeeze
`pg_squeeze` requiere configuración adicional en PostgreSQL:
```bash
# Instalar
sudo apt install postgresql-18-squeeze -y

# Habilitar en postgresql.conf
sudo -u postgres psql -c "ALTER SYSTEM SET wal_level = logical;"
sudo systemctl restart postgresql

# Instalar en la base de datos
sudo -u postgres psql -d tpch_squeeze -c "CREATE EXTENSION IF NOT EXISTS pg_squeeze;"
```

### Resultados de tamaño

| Base de datos | Tamaño | Notas |
|---|---|---|
| `tpch_base` | 12 GB | Baseline |
| `tpch_compresion` | 12 GB | LZ4 sin reducción visible* |
| `tpch_indices` | 43 GB | +31GB por índices |
| `tpch_indices_compresion` | 43 GB | LZ4 + índices |
| `tpch_squeeze` | 12 GB | pg_squeeze sin reducción visible* |

**\*Nota:** La compresión LZ4 y pg_squeeze no redujeron el tamaño en PostgreSQL porque
los campos TEXT de TPC-H son demasiado cortos (1-15 caracteres) para que LZ4 encuentre
patrones comprimibles, y los datos se cargaron en una sola operación `COPY` sin bloat.
En MongoDB sí funcionó Snappy porque comprime documentos BSON completos.

---

## Estructura del repositorio

```
10GB_station/
├── mongo10G/
│   ├── 1_baseline.py
│   ├── 2_compresion.py
│   ├── 3_indices.py
│   └── 4_indices_compresion.py
├── postgres10G/
│   ├── 1_base.py
│   ├── 2_compresion.py
│   ├── 3_indices.py
│   ├── 4_indices_compresion.py
│   ├── 5_compresion_pg_squeeze.py
│   └── 6_squeeze.py
└── README.md
```

---

## Funcion que calcula el consumo energetico: 
```
def trapezoid_energy(t, p):
    """
    Energía: E = ∫ P dt  (J).
    """
    t = np.asarray(t, dtype=float)
    p = np.asarray(p, dtype=float)
    mask = np.isfinite(t) & np.isfinite(p)
    t, p = t[mask], p[mask]

    if t.size < 2:
        return np.nan

    idx = np.argsort(t)
    t, p = t[idx], p[idx]

    if hasattr(np, "trapezoid"):
        return float(np.trapezoid(p, t))
    return float(np.trapz(p, t))
```
