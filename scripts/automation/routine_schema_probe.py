#!/usr/bin/env python3
"""Read-only logical-schema probe; host fingerprint mode needs only stdlib."""
import argparse
import hashlib
import json
import os
import re
import sys

SQL = '''
SELECT 'column' AS kind, n.nspname::text AS schema_name,
       c.relname::text AS relation_name, a.attname::text AS object_name,
       pg_catalog.format_type(a.atttypid,a.atttypmod) AS definition,
       a.attnotnull::text AS nullability,
       COALESCE(pg_get_expr(d.adbin,d.adrelid),'') AS default_expression
FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
JOIN pg_namespace n ON n.oid=c.relnamespace
LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
WHERE a.attnum>0 AND NOT a.attisdropped AND c.relkind IN ('r','p')
AND n.nspname='public' AND NOT c.relispartition
UNION ALL
SELECT 'constraint', n.nspname, c.relname, k.conname,
       pg_get_constraintdef(k.oid), '', ''
FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid
JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname='public' AND NOT c.relispartition
UNION ALL
SELECT 'index', schemaname, tablename, indexname, indexdef, '', ''
FROM pg_indexes i JOIN pg_class c ON c.relname=i.tablename
JOIN pg_namespace n ON n.oid=c.relnamespace AND n.nspname=i.schemaname
WHERE i.schemaname='public' AND NOT c.relispartition
ORDER BY 1,2,3,4,5,6,7
'''

def fingerprint(rows, cluster):
    if (not rows or not re.fullmatch(r'[0-9]+', str(cluster)) or
        any(not isinstance(row,list) or len(row)!=7 or
            any(not isinstance(value,str) for value in row) for row in rows)):
        raise ValueError('invalid_or_empty_live_schema')
    digest=hashlib.sha256(json.dumps(rows,separators=(',',':')).encode()).hexdigest()
    return {'schema_version':'qt.routine_schema.v1','schema_sha256':digest,
            'cluster_identifier':str(cluster)}

def sql_script():
    return '''BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout=10000;
SELECT json_build_object('rows', (
SELECT json_agg(json_build_array(kind,schema_name,relation_name,object_name,
definition,nullability,default_expression)) FROM (
''' + SQL + ''') AS logical_schema),
'cluster_identifier',(SELECT system_identifier::text FROM pg_control_system()));
COMMIT;
'''

def main():
    parser=argparse.ArgumentParser()
    mode=parser.add_mutually_exclusive_group()
    mode.add_argument('--sql',action='store_true')
    mode.add_argument('--fingerprint',action='store_true')
    args=parser.parse_args()
    if args.sql:
        print(sql_script())
        return
    if args.fingerprint:
        raw=sys.stdin.read(8*1024*1024+1)
        if len(raw)>8*1024*1024:
            raise ValueError('schema_probe_output_too_large')
        payload=json.loads(raw)
        print(json.dumps(fingerprint(payload['rows'],payload['cluster_identifier'])))
        return
    # Retained for existing backend-based operational use. Host controller uses
    # --sql/--fingerprint and the DB's existing local identity, not app health.
    from sqlalchemy import create_engine, text
    engine=create_engine(os.environ['PG_DSN'],connect_args={'connect_timeout':10})
    try:
        with engine.connect() as connection, connection.begin():
            connection.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
            connection.execute(text('SET LOCAL statement_timeout=10000'))
            rows=[list(row) for row in connection.execute(text(SQL))]
            cluster=str(connection.execute(text('SELECT system_identifier FROM pg_control_system()')).scalar_one())
            print(json.dumps(fingerprint(rows,cluster)))
    finally:
        engine.dispose()

if __name__=='__main__':
    main()
