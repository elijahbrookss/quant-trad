"""Compare two SQL representations of an offline synthetic immutable directory."""
from __future__ import annotations

import argparse
import json
import os
from random import Random
from statistics import median
from time import perf_counter_ns

from sqlalchemy import create_engine

from scripts.db.canonical_cold_benchmark import _db_url


def run(count):
    url = _db_url(os.environ.get("PG_DSN"))
    if url.database != "qt_bench_native_scale" or not 1000 <= count <= 1_000_000:
        raise ValueError("compact fixture requires its dedicated database and bounded rows")
    engine = create_engine(url.set(database="postgres"), connect_args={"options": "-c statement_timeout=120000"})
    layouts = {}
    try:
        with engine.connect() as conn, conn.begin():
            if conn.exec_driver_sql("SHOW listen_addresses").scalar_one() != "":
                raise RuntimeError("compact fixture requires a private non-networked PostgreSQL cluster")
            conn.exec_driver_sql(f"""
                CREATE TEMP TABLE sample AS SELECT i::bigint AS n,
                    'mfv_'||substr(md5('id'||i)||md5('ext'||i),1,40) AS id,
                    (i-1)/10000+1 AS page_id, mod((i-1),10000) AS ordinal,
                    mod(((i-1)/2),16)+1 AS series,
                    decode(md5('key'||((i-1)/2))||md5('k2'||((i-1)/2)), 'hex') AS key,
                    mod((i-1),2)+1 AS revision, decode(md5('hash'||i)||md5('h2'||i), 'hex') AS row_hash,
                    decode(md5('material'||i)||md5('m2'||i), 'hex') AS material,
                    'cfa_'||md5('page'||((i-1)/10000))||md5('p2'||((i-1)/10000)) AS page
                FROM generate_series(1,{count:d}) AS i;
            """)
            probes = Random(1729).sample(range(1, count+1), 128)
            witness = conn.exec_driver_sql("SELECT id,n,series,key,material,page,page_id,ordinal,row_hash FROM sample WHERE n=ANY(%s) ORDER BY n",
                                          (probes,)).all()
            for mode, id_type, page_type in (("current_keys", "text", "text"), ("binary_ids_integer_pages", "bytea", "bigint")):
                id_expression = "id" if id_type == "text" else "decode(substr(id,5),'hex')"
                page_expression = "page" if page_type == "text" else "page_id"
                conn.exec_driver_sql(f"""
                    CREATE TEMP TABLE loc (id {id_type} PRIMARY KEY,page {page_type} NOT NULL,ordinal integer NOT NULL,
                        series bigint NOT NULL,key bytea NOT NULL,revision integer NOT NULL,commit_seq bigint NOT NULL,
                        row_hash bytea NOT NULL,UNIQUE(page,ordinal));
                    CREATE INDEX ON loc(series,key,revision);
                    CREATE INDEX ON loc(series,commit_seq);
                    CREATE TEMP TABLE material (series bigint,hash bytea,canonical boolean,evidence bytea,id {id_type} REFERENCES loc,
                        PRIMARY KEY(series,hash,canonical,evidence,id));
                """)
                start = perf_counter_ns()
                conn.exec_driver_sql(f"INSERT INTO loc SELECT {id_expression},{page_expression},ordinal,series,key,revision,n,row_hash FROM sample")
                conn.exec_driver_sql(f"INSERT INTO material SELECT series,material,true,decode(repeat('00',32),'hex'),{id_expression} FROM sample")
                elapsed = (perf_counter_ns() - start) / 1e9
                sizes = conn.exec_driver_sql("SELECT pg_total_relation_size('loc'),pg_total_relation_size('material')").one()
                queries = {"exact": [], "key_head": [], "material": []}
                for identity, seq, series, key, digest, page, integer_page, ordinal, row_hash in witness:
                    encoded = identity if id_type == "text" else bytes.fromhex(identity[4:])
                    expected_page = page if page_type == "text" else integer_page
                    start = perf_counter_ns()
                    placement = conn.exec_driver_sql("SELECT page,ordinal,row_hash FROM loc WHERE id=%s", (encoded,)).one()
                    queries["exact"].append(perf_counter_ns() - start)
                    if (placement.page, placement.ordinal, bytes(placement.row_hash)) != (expected_page, ordinal, bytes(row_hash)):
                        raise RuntimeError("compact fixture exact lookup mismatch")
                    start = perf_counter_ns()
                    head = conn.exec_driver_sql("SELECT commit_seq FROM loc WHERE series=%s AND key=%s ORDER BY revision DESC LIMIT 1",
                                                (series, bytes(key))).scalar_one()
                    queries["key_head"].append(perf_counter_ns() - start)
                    if head != seq + seq % 2:
                        raise RuntimeError("compact fixture key head mismatch")
                    start = perf_counter_ns()
                    actual = conn.exec_driver_sql("SELECT id FROM material WHERE series=%s AND hash=%s AND canonical",
                                                  (series, bytes(digest))).scalar_one()
                    queries["material"].append(perf_counter_ns() - start)
                    if (actual if id_type == "text" else bytes(actual)) != encoded:
                        raise RuntimeError("compact fixture material lookup mismatch")
                layouts[mode] = {"locator_bytes": sizes[0], "material_bytes": sizes[1], "insert_seconds": elapsed,
                                 "warm_p50_ns": {name: median(samples) for name, samples in queries.items()}}
                conn.exec_driver_sql("DROP TABLE material,loc")
    finally:
        engine.dispose()
    return {"rows": count, "checks_per_lookup": 128, "layouts": layouts, "scope": "temporary_relations_synthetic_no_wal_no_publication"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100_000)
    print(json.dumps(run(parser.parse_args().rows), indent=2, sort_keys=True))
