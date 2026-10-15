#!/usr/bin/env python3
"""Read-only live schema fingerprint, sent to the existing backend via stdin."""
import hashlib
import json
import os
from sqlalchemy import create_engine, text

SQL = '''
SELECT 'column', n.nspname, c.relname, a.attname,
       pg_catalog.format_type(a.atttypid,a.atttypmod), a.attnotnull::text,
       COALESCE(pg_get_expr(d.adbin,d.adrelid),'')
FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
JOIN pg_namespace n ON n.oid=c.relnamespace
LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
WHERE a.attnum>0 AND NOT a.attisdropped AND c.relkind IN ('r','p')
AND n.nspname = 'public' AND NOT c.relispartition
UNION ALL
SELECT 'constraint', n.nspname, c.relname, k.conname,
       pg_get_constraintdef(k.oid), '', ''
FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid
JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname = 'public' AND NOT c.relispartition
UNION ALL
SELECT 'index', schemaname, tablename, indexname, indexdef, '', ''
FROM pg_indexes i JOIN pg_class c ON c.relname=i.tablename
JOIN pg_namespace n ON n.oid=c.relnamespace AND n.nspname=i.schemaname
WHERE i.schemaname='public' AND NOT c.relispartition
ORDER BY 1,2,3,4,5,6,7
'''

def main():
    engine=create_engine(os.environ['PG_DSN'],connect_args={'connect_timeout':10})
    try:
        with engine.connect() as connection, connection.begin():
            connection.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
            connection.execute(text('SET LOCAL statement_timeout = 10000'))
            rows=[list(row) for row in connection.execute(text(SQL))]
            cluster=str(connection.execute(text('SELECT system_identifier FROM pg_control_system()')).scalar_one())
            digest=hashlib.sha256(json.dumps(rows,separators=(',',':')).encode()).hexdigest()
            print(json.dumps({'schema_version':'qt.routine_schema.v1','schema_sha256':digest,'cluster_identifier':cluster}))
    finally:
        engine.dispose()

if __name__=='__main__':
    main()
