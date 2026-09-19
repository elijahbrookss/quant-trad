"""Fixed SSD/HDD placement for the preserving header copy.

No device preparation, allocator or runtime policy. Fixed tablespace creation reuses
the existing PostgreSQL process/namespace verifier. The operator must separately
budget capacity, WAL and the full migration duration.
"""
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
import json
import os
import stat
from time import monotonic

from sqlalchemy import text

from core.storage_targets import StorageTarget
from portal.backend.service.storage.header_filesystem import (
    PostgresProcessObservation, _check_control_binary, _cluster_identity,
    _device_id, _same_process_file, _process_readlink,
)


@dataclass(frozen=True)
class CopyPlacement:
    recent: StorageTarget
    history: StorageTarget
    history_tablespace_oid: int
    history_before: date
    pg_controldata: Path

    def __post_init__(self):
        if (not isinstance(self.recent,StorageTarget) or not isinstance(self.history,StorageTarget)
                or self.recent.medium!="ssd" or self.history.medium!="hdd"
                or self.recent.state!="active" or self.history.state!="active"
                or "recent" not in self.recent.roles or "history" not in self.history.roles
                or self.recent.target_id==self.history.target_id
                or self.recent.filesystem_uuid==self.history.filesystem_uuid):
            raise ValueError("fact_header_copy_requires_distinct_recent_ssd_and_history_hdd")
        if (type(self.history_tablespace_oid) is not int
                or not 1<=self.history_tablespace_oid<=2**32-1
                or self.history_tablespace_oid in (1663,1664)
                or type(self.history_before) is not date
                or not isinstance(self.pg_controldata,Path) or not self.pg_controldata.is_absolute()):
            raise ValueError("fact_header_copy_placement_invalid")

    def describe(self):
        return json.loads(json.dumps({
            "recent":asdict(self.recent),"history":asdict(self.history),
            "history_tablespace_oid":self.history_tablespace_oid,
            "history_before":self.history_before.isoformat(),
            "pg_controldata":str(self.pg_controldata),
        }))


def _restore(description):
    values=dict(description)
    for role in ("recent","history"):
        target=dict(values[role])
        target["roles"]=tuple(target["roles"])
        values[role]=StorageTarget(**target)
    values["history_before"]=date.fromisoformat(values["history_before"])
    values["pg_controldata"]=Path(values["pg_controldata"])
    return CopyPlacement(**values)


def _observe_database(conn, pg_controldata, deadline):
    context=conn.execute(text("""
        SELECT clock_timestamp() AS captured_at,current_setting('data_directory') AS root,
          pg_postmaster_start_time() AS started,pg_read_file('postmaster.pid',0,4096) AS pidfile,
          current_setting('server_version_num')::int AS version,
          c.system_identifier::text||'/'||d.oid::text AS identity,
          c.catalog_version_no::int AS catalog_version,d.dattablespace::bigint AS default_space,
          d.oid::bigint AS database_oid
        FROM pg_control_system() c CROSS JOIN pg_database d WHERE d.datname=current_database()
    """)).mappings().one()
    if context["version"]//10000!=15 or context["default_space"]!=1663:
        raise RuntimeError("fact_header_copy_fixed_default_storage_required")
    binary=pg_controldata.resolve(strict=True)
    _check_control_binary(binary,deadline)
    process=PostgresProcessObservation(context["identity"],context["captured_at"],context["root"],
        context["started"],tuple(context["pidfile"].splitlines()[:3]))
    cluster=_cluster_identity(process,binary,deadline)
    root,pid=cluster[:2]
    return context, process, cluster, binary


def observe(conn, plan, *, deadline=None):
    """Bind two configured roots and an existing history tablespace to this PG."""
    if not isinstance(plan,CopyPlacement):
        raise ValueError("fact_header_copy_placement_invalid")
    deadline=min(monotonic()+30,deadline) if deadline is not None else monotonic()+30
    context,process,cluster,binary=_observe_database(conn,plan.pg_controldata,deadline)
    root,pid=cluster[:2]
    destination=conn.execute(text("""
        SELECT oid::bigint AS oid,spcname,pg_tablespace_location(oid) AS location,
               has_tablespace_privilege(oid,'CREATE') AS can_create
        FROM pg_tablespace WHERE oid=:oid
    """),{"oid":plan.history_tablespace_oid}).mappings().one_or_none()
    if destination is None or not destination["can_create"]:
        raise RuntimeError("fact_header_copy_history_tablespace_unavailable")
    roots={}
    capacities={}
    for role in ("recent","history"):
        target=getattr(plan,role)
        capacity=target.inspect(require_writable=True)
        resolved=Path(target.root).resolve(strict=True)
        if str(resolved)!=capacity.path or _device_id(resolved.stat().st_dev)!=capacity.device_id:
            raise RuntimeError("fact_header_copy_target_changed")
        _same_process_file(pid,Path(target.root),resolved.stat())
        roots[role]=resolved
        capacities[role]=capacity
    if capacities["recent"].device_id==capacities["history"].device_id:
        raise RuntimeError("fact_header_copy_distinct_filesystems_required")
    if (not root.is_relative_to(roots["recent"])
            or _device_id(root.stat().st_dev)!=capacities["recent"].device_id):
        raise RuntimeError("fact_header_copy_database_not_on_recent_ssd")
    declared=Path(destination["location"])
    link=root/"pg_tblspc"/str(plan.history_tablespace_oid)
    if (not declared.is_absolute() or ".." in declared.parts or not link.is_symlink()
            or link.readlink()!=declared or _process_readlink(pid,link)!=str(declared)):
        raise RuntimeError("fact_header_copy_history_tablespace_link_changed")
    directory=declared/f"PG_15_{context['catalog_version']}"
    actual=directory.resolve(strict=True)
    info=actual.stat()
    if (directory.is_symlink() or not actual.is_relative_to(roots["history"])
            or _device_id(info.st_dev)!=capacities["history"].device_id):
        raise RuntimeError("fact_header_copy_history_tablespace_wrong_filesystem")
    _same_process_file(pid,declared,declared.stat())
    _same_process_file(pid,directory,info,require_writable=True)
    if _cluster_identity(process,binary,deadline)!=cluster:
        raise RuntimeError("fact_header_copy_database_process_changed")
    for role in ("recent","history"):
        current=getattr(plan,role).inspect(require_writable=True)
        if (current.device_id!=capacities[role].device_id or current.read_only
                or current.filesystem_uuid!=capacities[role].filesystem_uuid
                or Path(getattr(plan,role).root).resolve(strict=True)!=roots[role]):
            raise RuntimeError("fact_header_copy_target_changed")
    if directory.stat().st_ino!=info.st_ino or link.readlink()!=declared:
        raise RuntimeError("fact_header_copy_history_tablespace_changed")
    _same_process_file(pid,directory,info,require_writable=True)
    if monotonic()>=deadline:
        raise RuntimeError("fact_header_copy_placement_time_budget_exceeded")
    binding={"plan":plan.describe(),"database_identity":context["identity"],
             "database_root":str(root),"database_oid":context["database_oid"],
             "recent_device":capacities["recent"].device_id,
             "history_device":capacities["history"].device_id,
             "history_name":destination["spcname"],"history_location":str(declared),
             "history_directory":str(actual),"history_directory_inode":info.st_ino,
             "catalog_version":context["catalog_version"]}
    return binding,pid


def verify(conn, saved):
    observed,pid=observe(conn,_restore(saved["plan"]))
    if observed!=saved:
        raise RuntimeError("fact_header_copy_placement_binding_changed")
    return pid


@contextmanager
def tablespace(conn, name):
    previous=conn.scalar(text("SHOW default_tablespace"))
    conn.execute(text("SELECT set_config('default_tablespace',:name,true)"),{"name":name})
    yield
    # On failure the enclosing copy savepoint restores this setting with DDL.
    conn.execute(text("SELECT set_config('default_tablespace',:name,true)"),{"name":previous})


def verify_group(conn, relation, *, history, saved, pid):
    """Verify heap, ordinary indexes, TOAST and TOAST indexes on the serving disk."""
    members=conn.execute(text("""
        WITH heap AS (SELECT oid,reltoastrelid FROM pg_class WHERE oid=to_regclass(:relation)),
        heaps AS (SELECT oid FROM heap UNION ALL SELECT reltoastrelid FROM heap WHERE reltoastrelid<>0),
        members AS (SELECT oid FROM heaps UNION ALL SELECT indexrelid FROM pg_index WHERE indrelid IN (SELECT oid FROM heaps))
        SELECT c.oid,c.relkind,c.relpersistence,c.relfilenode,
               COALESCE(NULLIF(c.reltablespace,0),d.dattablespace)::bigint AS space,
               pg_relation_filepath(c.oid) AS path
        FROM members JOIN pg_class c ON c.oid=members.oid
        CROSS JOIN pg_database d WHERE d.datname=current_database() LIMIT 129
    """),{"relation":relation}).mappings().all()
    if not members or len(members)>128:
        raise RuntimeError("fact_header_copy_placement_relation_inventory_invalid")
    space=saved["plan"]["history_tablespace_oid"] if history else 1663
    root=Path(saved["database_root"])
    for member in members:
        if member["relkind"] in ("p","I"):
            continue
        if member["relkind"] not in ("r","i","t") or member["relpersistence"]!="p" or member["space"]!=space:
            raise RuntimeError("fact_header_copy_relation_on_wrong_tablespace: "+relation)
        if history:
            relative=f"pg_tblspc/{space}/PG_15_{saved['catalog_version']}/{saved['database_oid']}/{member['relfilenode']}"
            serving=Path(saved["history_directory"])/str(saved["database_oid"])/str(member["relfilenode"])
        else:
            relative=f"base/{saved['database_oid']}/{member['relfilenode']}"
            serving=root/relative
        if member["path"]!=relative:
            raise RuntimeError("fact_header_copy_relation_path_changed")
        path=root/relative
        info=path.stat()
        if path.is_symlink() or not stat.S_ISREG(info.st_mode):
            raise RuntimeError("fact_header_copy_relation_file_invalid")
        if _device_id(info.st_dev)!=saved["history_device" if history else "recent_device"]:
            raise RuntimeError("fact_header_copy_relation_on_wrong_filesystem")
        _same_process_file(pid,serving,info)


def prepare_history_tablespace(engine, *, recent, history, history_before,
                               pg_controldata, timeout_seconds=60):
    """Prepare only the first HDD destination, preserving all existing files.

    Runs in the same verified PostgreSQL filesystem/PID namespace as migration.
    The fixed child directory must be privately owned by PostgreSQL, or its
    parent must permit this same unprivileged user to create it. No chmod,
    chown, mount, format, file deletion or relation movement is performed.
    Existing catalog identity resolves an uncertain CREATE response on retry.
    """
    if (not isinstance(recent, StorageTarget) or not isinstance(history, StorageTarget)
            or recent.medium != "ssd" or history.medium != "hdd"
            or recent.state != "active" or history.state != "active"
            or "recent" not in recent.roles or "history" not in history.roles
            or recent.target_id == history.target_id
            or recent.filesystem_uuid == history.filesystem_uuid
            or type(history_before) is not date
            or not isinstance(pg_controldata, Path) or not pg_controldata.is_absolute()
            or type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 300):
        raise ValueError("fact_header_history_preparation_inputs_invalid")
    deadline = monotonic()+timeout_seconds
    name = "qt_history_"+history.target_id
    directory = Path(history.root)/"postgres"
    # CREATE TABLESPACE cannot run in a transaction. This private connection
    # owns a session lock and is discarded on every exit, including uncertainty.
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        try:
            conn.execute(text("SELECT set_config('statement_timeout',:value,false)"),
                         {"value": str(timeout_seconds*1000)})
            if not conn.scalar(text("SELECT pg_try_advisory_lock("
                                    "hashtextextended('quant-trad:fact-header-cutover:v2',0))")):
                raise RuntimeError("fact_header_history_preparation_busy")
            context, process, cluster, binary = _observe_database(conn,pg_controldata,deadline)
            database_root, pid = cluster[:2]

            def roots():
                capacities = {}
                for target in (recent,history):
                    evidence = target.inspect(require_writable=True)
                    path = Path(target.root)
                    if path.resolve(strict=True) != path:
                        raise RuntimeError("fact_header_history_preparation_canonical_root_required")
                    _same_process_file(pid,path,path.stat())
                    capacities[target.target_id] = evidence
                if (capacities[recent.target_id].device_id == capacities[history.target_id].device_id
                        or not database_root.is_relative_to(Path(recent.root))
                        or _device_id(database_root.stat().st_dev) != capacities[recent.target_id].device_id):
                    raise RuntimeError("fact_header_history_preparation_source_binding_changed")
                if _cluster_identity(process,binary,deadline) != cluster:
                    raise RuntimeError("fact_header_history_preparation_database_changed")
                if monotonic() >= deadline:
                    raise RuntimeError("fact_header_history_preparation_time_budget_exceeded")
                return capacities

            capacities = roots()
            row = conn.execute(text("""
                SELECT oid::bigint AS oid,pg_tablespace_location(oid) AS location,
                    spcowner=(SELECT oid FROM pg_roles WHERE rolname=current_user) AS owned
                FROM pg_tablespace WHERE spcname=:name
            """), {"name": name}).mappings().one_or_none()
            if row is not None and (row["location"] != str(directory) or not row["owned"]):
                raise RuntimeError("fact_header_history_preparation_catalog_mismatch")
            if not directory.exists():
                if directory.is_symlink() or row is not None:
                    raise RuntimeError("fact_header_history_preparation_directory_missing")
                directory.mkdir(mode=0o700)
                parent = os.open(directory.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
                try:
                    os.fsync(parent)
                finally:
                    os.close(parent)
            info = directory.lstat()
            if (not stat.S_ISDIR(info.st_mode) or directory.resolve(strict=True) != directory
                    or _device_id(info.st_dev) != capacities[history.target_id].device_id):
                raise RuntimeError("fact_header_history_preparation_directory_changed")
            _same_process_file(pid,directory,info,require_writable=True)
            if row is None:
                if next(directory.iterdir(),None) is not None:
                    raise RuntimeError("fact_header_history_preparation_nonempty_unregistered_directory")
                roots()
                conn.execute(text("SELECT set_config('statement_timeout',:value,false)"),
                             {"value": str(max(1,int((deadline-monotonic())*1000)))})
                # Driver quoting is required: names/paths never become shell text.
                from psycopg2 import sql
                statement = sql.SQL("CREATE TABLESPACE {} LOCATION {}").format(
                    sql.Identifier(name),sql.Literal(str(directory)))
                conn.exec_driver_sql(statement.as_string(conn.connection.driver_connection))
                row = conn.execute(text(
                    "SELECT oid::bigint AS oid FROM pg_tablespace WHERE spcname=:name"),
                    {"name": name}).mappings().one()
            roots()
            plan = CopyPlacement(recent,history,row["oid"],history_before,pg_controldata)
            observe(conn,plan,deadline=deadline)
            if monotonic() >= deadline:
                raise RuntimeError("fact_header_history_preparation_time_budget_exceeded")
            return plan
        finally:
            # Session-lock and timeout state must never escape into the pool.
            conn.invalidate()
