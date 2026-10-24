"""Existing disposable PostgreSQL only; no production credentials or science."""
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import delete, text

from portal.backend.db.models import AsyncJobRecord, ResearchItemRecord
from portal.backend.db.session import db
from portal.backend.service.research import operator_reads as ops, repository
from portal.backend.service.research.async_dispatch import JOB_TYPE_RESEARCH_CHECK_RUN, research_request_fingerprint
from portal.backend.service.research.execution_limits import _GLOBAL_LOCK
from tests.test_portal.test_saved_selection_measurements import synthetic

pytestmark=pytest.mark.db


def test_saved_request_preparation_reads_persisted_research_item():
    item,_,ctx,_=synthetic();key=str(uuid4());item['id']=key
    try:
        with db.session()as session:
            session.add(ResearchItemRecord(**{k:item[k]for k in
                ('id','kind','status','instrument_id','timeframe','payload')},title='Disposable saved metadata'))
        raw=repository.get_item(key)
        assert 'replayable' not in raw and raw['payload']['replayable'] is True
        result=ops.prepare_saved_selection_measurement_request(key)
        assert result['source_check_id']==key and result['frozen_context']==ctx
        assert result['request']['outcomes']['selection_reference']['source_check_id']==key
    finally:
        with db.session()as session:session.execute(delete(ResearchItemRecord).where(ResearchItemRecord.id==key))


def test_snapshot_observes_real_lock_terminal_duplicates_and_existing_status():
    prefix='operator-job-'+uuid4().hex
    request={'scope':{'marker':prefix},'mode':'evidence','detector':{}}
    fingerprint=research_request_fingerprint(job_type=JOB_TYPE_RESEARCH_CHECK_RUN,request=request)
    keys=[prefix+str(i)for i in range(3)]
    try:
        with db.session()as session:
            for key,status in zip(keys,['failed','succeeded','queued']):
                session.add(AsyncJobRecord(id=key,job_type=JOB_TYPE_RESEARCH_CHECK_RUN,status=status,
                    request_fingerprint=fingerprint,attempts=1 if status!='queued'else 0,max_attempts=1,
                    payload={'request':request},error='fixture failure'if status=='failed'else None))
        with db.session()as owner:
            owner.execute(text('SELECT pg_advisory_xact_lock(hashtextextended(:name,0))'),{'name':_GLOBAL_LOCK})
            result=ops.execution_snapshot(request=request,job_id=keys[0],limit=1)
            assert result['research_owner_held'] and not result['admission_granted']
            assert result['truncated']['matching_jobs'] and len(result['matching_jobs'])==1
            assert result['matching_jobs'][0]['job_id']in keys
            assert result['requested_job']['status']=='failed'
            assert result['requested_job']['attempts']==result['requested_job']['max_attempts']==1
            assert result['requested_job']['schema_version']=='research_job_status.v1'
        after=ops.execution_snapshot(request=request,limit=100)
        assert {row['job_id']for row in after['matching_jobs']}==set(keys)
        assert keys[2]in{row['job_id']for row in after['global_inflight_jobs']}
    finally:
        with db.session()as session:session.execute(delete(AsyncJobRecord).where(AsyncJobRecord.id.in_(keys)))
