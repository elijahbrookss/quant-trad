#!/usr/bin/env python3
"""Fail-closed, frontend-only compatible promotion using existing host controller.

Never loads a database, runs a migration, starts services, or creates credentials.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile
import zipfile
from datetime import datetime, timezone

from check_release_ci import qualify, api

def recovery_admission(state, previous, schema=None):
    """Validate the operator's private, source-bound restore qualification.

    This consumes evidence from the approved backup procedure; it never fabricates
    a backup or claims the hashes alone prove a successful restoration.
    """
    receipt_path=state/'routine-recovery.json'
    if receipt_path.is_symlink() or receipt_path.stat().st_mode & 0o077:
        raise ValueError('routine recovery receipt must be private and not a symlink')
    receipt=json.loads(receipt_path.read_text())
    now=datetime.now(timezone.utc)
    try:
        verified=datetime.fromisoformat(receipt['verified_at'].replace('Z','+00:00'))
        expires=datetime.fromisoformat(receipt['expires_at'].replace('Z','+00:00'))
    except (KeyError,ValueError,TypeError):
        raise ValueError('invalid recovery qualification time') from None
    if (receipt.get('schema_version')!='qt.routine_recovery.v1' or
        receipt.get('revision')!=previous or receipt.get('restore_result')!='passed' or
        not verified <= now < expires or (expires-verified).total_seconds()>86400):
        raise ValueError('recovery qualification absent, stale, or incompatible')
    for field in ('backup_manifest','restore_receipt'):
        evidence=Path(receipt[field])
        if not evidence.is_absolute() or evidence.is_symlink() or not evidence.is_file():
            raise ValueError('recovery evidence unavailable')
        with evidence.open('rb') as stream:
            actual=hashlib.file_digest(stream,'sha256').hexdigest()
        if receipt.get(field+'_sha256')!=actual:
            raise ValueError('recovery evidence changed')
    if schema is not None and (schema.get('schema_version')!='qt.routine_schema.v1' or
        not re.fullmatch(r'[a-f0-9]{64}', schema.get('schema_sha256','')) or
        not re.fullmatch(r'[0-9]+', str(schema.get('cluster_identifier',''))) or
        receipt.get('schema_sha256')!=schema['schema_sha256'] or
        receipt.get('cluster_identifier')!=schema.get('cluster_identifier')):
        raise ValueError('live schema or cluster differs from qualified recovery point')
    return receipt

def output(*args):
    return subprocess.run(args,check=True,capture_output=True,text=True,timeout=60).stdout.strip()

def eligible(paths):
    # Smaller than CI selection: no backend, dependency, API adapter, registry,
    # serialization, runtime, config, migration, security or storage changes.
    if not paths:
        raise ValueError('routine promotion has no reviewed changes')
    for path in paths:
        docs=path.startswith('docs/') and path.endswith(('.md','.png','.jpg','.svg','.mmd'))
        ui=path.startswith(('portal/frontend/src/','portal/frontend-v2/src/')) and path.endswith(('.jsx','.tsx','.css','.scss','.png','.jpg','.svg'))
        if (not (docs or ui) or '..' in path.split('/') or
            any(p in path.split('/') for p in ('api','adapters','services','auth','security'))):
            raise ValueError(f'routine compatibility unknown for {path}; use reviewed manual promotion')

def verify_package(package, revision, receipt):
    artifact_response=api(f"actions/runs/{receipt['run_id']}/artifacts?per_page=100")
    if artifact_response['total_count'] != len(artifact_response['artifacts']) or artifact_response['total_count']>100:
        raise ValueError('incomplete artifact response')
    matches=[a for a in artifact_response['artifacts'] if a['name']=='routine-release-package' and not a['expired']]
    if len(matches)!=1:
        raise ValueError('qualified release package missing or expired')
    artifact=matches[0]
    if artifact.get('workflow_run',{}).get('head_sha')!=revision or artifact.get('workflow_run',{}).get('id')!=receipt['run_id']:
        raise ValueError('package source identity mismatch')
    with package.open('rb') as stream:
        digest='sha256:'+hashlib.file_digest(stream,'sha256').hexdigest()
    if artifact.get('digest')!=digest:
        raise ValueError('package differs from Actions artifact digest')
    jobs=api(f"actions/runs/{receipt['run_id']}/attempts/{receipt['attempt']}/jobs?per_page=100")
    matches=[j for j in jobs['jobs'] if j['name']=='deployment-contract']
    if len(matches)!=1 or not (matches[0]['started_at'] <= artifact['created_at'] <= matches[0]['completed_at']):
        raise ValueError('package not bound to qualifying workflow attempt')
    steps={s['name']:s.get('conclusion') for s in matches[0]['steps']}
    if steps.get('Export qualified routine images')!='success' or steps.get('Preserve qualified routine images')!='success':
        raise ValueError('image export not qualified')
    with zipfile.ZipFile(package) as archive:
        if sorted(archive.namelist())!=['images.tar','manifest.json']:
            raise ValueError('unexpected package contents')
        manifest=json.loads(archive.read('manifest.json'))
        if (manifest.get('schema_version')!='qt.release_package.v1' or manifest.get('revision')!=revision or
            manifest.get('run_id')!=receipt['run_id'] or manifest.get('attempt')!=receipt['attempt'] or
            not re.fullmatch(r'[a-f0-9]{64}',manifest.get('source_tree_hash',''))):
            raise ValueError('invalid package manifest')
        if set(manifest.get('images',{}))!={'backend','frontend','frontend-v2'}:
            raise ValueError('incomplete application package')
        for name, image in manifest['images'].items():
            if image.get('tag')!=f'quanttrad-{name}:{revision}' or not re.fullmatch(r'sha256:[a-f0-9]{64}',image.get('id','')):
                raise ValueError('invalid package image')
        with archive.open('images.tar') as stream:
            if hashlib.file_digest(stream,'sha256').hexdigest()!=manifest.get('archive_sha256'):
                raise ValueError('image archive checksum mismatch')
    return manifest

def pin_candidate(model, manifest):
    model=json.loads(json.dumps(model))
    for name,service in model['services'].items():
        image=service.get('image','')
        match=next((v for v in manifest['images'].values() if v['tag']==image),None)
        if match:
            service['image']=match['id']
        else:
            # Infrastructure stays on the actually running image. No remote pulls.
            ids=output('docker','ps','--all','--quiet','--filter',f"label=com.docker.compose.project={model['name']}",
                       '--filter',f'label=com.docker.compose.service={name}','--filter','label=com.docker.compose.oneoff=False').splitlines()
            if len(ids)!=1:
                raise ValueError(f'routine promotion requires one existing {name}')
            service['image']=output('docker','inspect','--format','{{.Image}}',ids[0])
            if not re.fullmatch(r'sha256:[a-f0-9]{64}',service['image']):
                raise ValueError('invalid retained infrastructure image')
        service.pop('build',None)
        service['pull_policy']='never'
    return model

def main():
    p=argparse.ArgumentParser()
    p.add_argument('action',choices=['admit','pin'])
    p.add_argument('--candidate',required=True)
    p.add_argument('--previous',required=True)
    p.add_argument('--package',type=Path,required=True)
    p.add_argument('--state',type=Path,required=True)
    p.add_argument('--config',type=Path)
    p.add_argument('--schema',type=Path)
    args=p.parse_args()
    if not all(re.fullmatch(r'[a-f0-9]{40}',x) for x in (args.candidate,args.previous)):
        raise ValueError('full candidate and rollback revisions required')
    if args.action=='admit':
        subprocess.run(['git','merge-base','--is-ancestor',args.previous,args.candidate],check=True,timeout=30)
        paths=output('git','diff','--name-only','--no-renames',args.previous,args.candidate).splitlines()
        eligible(paths)
        if args.schema is None:
            raise ValueError('read-only live schema preflight is required')
        recovery_admission(args.state,args.previous,json.loads(args.schema.read_text()))
        receipt=qualify(args.candidate)
        manifest=verify_package(args.package,args.candidate,receipt)
        # The unchanged backend/config/schema/registry/write/archive readers are
        # the compatibility proof for this deliberately narrow eligibility set.
        # Current release is healthy and source-attested in the existing controller.
        with tempfile.TemporaryDirectory(prefix='qt-routine-package-') as directory:
            archive_path=Path(directory)/'images.tar'
            with zipfile.ZipFile(args.package) as archive, archive.open('images.tar') as source, archive_path.open('wb') as dest:
                import shutil
                shutil.copyfileobj(source,dest)
            subprocess.run(['docker','load','--input',str(archive_path)],check=True,timeout=600)
        for value in manifest['images'].values():
            if output('docker','image','inspect','--format','{{.Id}}',value['tag'])!=value['id']:
                raise ValueError('loaded image identity mismatch')
        actual=output('docker','run','--rm','--network','none','--read-only','--entrypoint','cat',manifest['images']['backend']['id'],'/app/.qt-source-attestation.json')
        attestation=json.loads(actual)
        if attestation.get('source_revision')!=args.candidate or attestation.get('source_tree_hash')!=manifest['source_tree_hash']:
            raise ValueError('loaded backend source attestation mismatch')
        (args.state/'routine-manifest.json').write_text(json.dumps(manifest)+'\n')
        print(json.dumps({'event':'routine_compatibility_admitted','candidate':args.candidate,'rollback':args.previous,'paths':paths,
                          'scope':'unchanged backend, schema, registry, serialization, dependencies, configuration and storage'}))
    else:
        manifest=json.loads((args.state/'routine-manifest.json').read_text())
        if manifest['revision']!=args.candidate:
            raise ValueError('candidate manifest changed')
        model=pin_candidate(json.loads(args.config.read_text()),manifest)
        args.config.write_text(json.dumps(model)+'\n')

if __name__=='__main__':
    main()
