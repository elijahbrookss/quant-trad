"""Bounded read-only operator support; never admission, allocation or execution."""
from copy import deepcopy
from datetime import datetime

from sqlalchemy import select, text

from core.execution_control import execution_checkpoint
from portal.backend.db.models import AsyncJobRecord
from portal.backend.db.session import db
from research_science.check import CheckEvidenceBinding, CheckRequest, CheckResult
from . import repository
from .async_dispatch import (JOB_TYPE_RESEARCH_CHECK_RUN, get_research_job_status,
                             research_request_fingerprint)
from .execution_limits import _GLOBAL_LOCK
from .saved_selection_binding import load_saved_selection, saved_context, invalid


def _readonly(session):
    session.execute(text("SET TRANSACTION READ ONLY"))
    session.execute(text("SET LOCAL statement_timeout='5s'"))
    session.execute(text("SET LOCAL lock_timeout='250ms'"))


def prepare_saved_selection_measurement_request(source_check_id, *, measurement_profile='18', expected_hashes=None):
    """Validate saved metadata only; no candle loading, provider or science."""
    if measurement_profile != '18':
        raise ValueError('saved_measurement_profile_unsupported: only 18 is admitted')
    expected_hashes = {} if expected_hashes is None else expected_hashes
    allowed = {'result_hash', 'evidence_hash', 'selection_manifest_hash'}
    if not isinstance(expected_hashes, dict) or set(expected_hashes)-allowed:
        raise ValueError('saved_measurement_expected_hashes_invalid')
    with db.session() as session:
        _readonly(session)
        item = repository.get_item(source_check_id, session=session)
        payload = item['payload']
        result = CheckResult.from_dict(payload['result'])
        evidence = CheckEvidenceBinding.from_dict(payload['evidence'])
        request = CheckRequest.from_dict(payload['request'])
        reference = {'source_check_id':item['id'], 'result_hash':result.result_hash,
            'evidence_hash':evidence.evidence_hash,
            'selection_manifest_hash':result.result['selection_manifest_hash']}
        if any(value != reference[key] for key,value in expected_hashes.items()):
            invalid('expected saved hashes differ')
        context = saved_context(result.result, evidence.input_binding, request)
        saved = load_saved_selection(reference, context, item_loader=lambda key:item,
            checkpoint=execution_checkpoint, admitted_versions=('12','14','17'))
    from .saved_selection_measurements_evaluator import DETECTOR, REUSABLE_SCHEMA
    ready = {'check_family':'raw_forward_outcome','mode':'evidence','dataset_id':saved.context['dataset_id'],
        'scope':{'instrument_id':saved.context['instrument_id'],'timeframe':'15m',
            'start':saved.context['decision_start'],'end':saved.context['decision_end_exclusive']},
        'detector':deepcopy(DETECTOR),'gap_policy':'reject',
        'outcomes':{'schema_version':REUSABLE_SCHEMA,'selection_reference':saved.reference,
                    'frozen_context':saved.context}}
    return {'schema_version':'research.saved_measurement_request.v1','measurement_profile':'18',
        'source_check_id':saved.reference['source_check_id'],'selection_reference':saved.reference,
        'frozen_context':saved.context,'request':ready,'read_only':True,
        'scientific_execution_performed':False}


def _job_rows(session, condition, limit):
    fields = (AsyncJobRecord.id, AsyncJobRecord.job_type, AsyncJobRecord.status,
        AsyncJobRecord.request_fingerprint, AsyncJobRecord.attempts, AsyncJobRecord.max_attempts,
        AsyncJobRecord.created_at, AsyncJobRecord.started_at, AsyncJobRecord.finished_at,
        AsyncJobRecord.error)
    rows = session.execute(select(*fields).where(condition).order_by(
        AsyncJobRecord.created_at.desc(),AsyncJobRecord.id.desc()).limit(limit+1)).mappings().all()
    return [{('job_id' if key=='id' else key):
        (value.isoformat()+'Z' if isinstance(value,datetime) else value)
        for key,value in row.items()} for row in rows[:limit]], len(rows)>limit


def execution_snapshot(*, request=None, job_id=None, limit=25):
    """Observe existing jobs/owner; this is never a reservation or retry grant."""
    if type(limit) is not int or not 1<=limit<=100:
        raise ValueError('execution_snapshot_limit_invalid: use 1..100')
    if request is not None and not isinstance(request,dict):
        raise ValueError('execution_snapshot_request_must_be_object')
    fingerprint = (research_request_fingerprint(job_type=JOB_TYPE_RESEARCH_CHECK_RUN,request=request)
                   if request is not None else None)
    with db.session() as session:
        _readonly(session)
        observed_at = session.scalar(text('SELECT clock_timestamp()'))
        owner_held = session.scalar(text("""
            WITH key AS (SELECT hashtextextended(:name,0) AS value)
            SELECT EXISTS(SELECT 1 FROM pg_locks,key WHERE locktype='advisory'
                AND mode='ExclusiveLock' AND granted AND objsubid=1
                AND classid::bigint=((key.value>>32)&4294967295)
                AND objid::bigint=(key.value&4294967295))
        """), {'name':_GLOBAL_LOCK})
        inflight, inflight_truncated = _job_rows(session,
            AsyncJobRecord.status.in_(['queued','running','retry']),limit)
        matching, matching_truncated = ([],False) if fingerprint is None else _job_rows(session,
            (AsyncJobRecord.job_type==JOB_TYPE_RESEARCH_CHECK_RUN)
            & (AsyncJobRecord.request_fingerprint==fingerprint),limit)
    requested = get_research_job_status(job_id) if job_id is not None else None
    return {'schema_version':'research.execution_snapshot.v1','observed_at':observed_at.isoformat(),
        'global_inflight_jobs':inflight,'research_owner_held':bool(owner_held),
        'request_fingerprint':fingerprint,'matching_jobs':matching,'requested_job':requested,
        'limit':limit,'truncated':{'global_inflight_jobs':inflight_truncated,'matching_jobs':matching_truncated},
        'read_only':True,'admission_granted':False,
        'caveats':['Point-in-time diagnostic; jobs and ownership can change before dispatch.',
            'Exact request fingerprint matching uses the existing dispatch contract, not scientific equivalence.',
            'Requested job uses the existing status reader and can be observed slightly later.',
            'No attempt allocation, ledger write, retry authority or reservation is created.']}
