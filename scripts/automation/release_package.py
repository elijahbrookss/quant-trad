#!/usr/bin/env python3
"""Capture and export the unchanged images exercised by deployment-contract."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

IMAGES = ('backend', 'frontend', 'frontend-v2')

def output(*args):
    return subprocess.run(args, check=True, capture_output=True, text=True, timeout=60).stdout.strip()

def image_ids(revision):
    if not re.fullmatch(r'[a-f0-9]{40}', revision):
        raise ValueError('full source revision required')
    result = {}
    for name in IMAGES:
        tag = f'quanttrad-{name}:{revision}'
        identity = output('docker','image','inspect','--format','{{.Id}}',tag)
        if not re.fullmatch(r'sha256:[a-f0-9]{64}', identity):
            raise ValueError('invalid image identity')
        result[name] = {'tag':tag, 'id':identity}
    return result

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('action',choices=['capture','export'])
    parser.add_argument('--root',type=Path,required=True)
    args=parser.parse_args()
    root=args.root
    if args.action=='capture':
        root.mkdir(parents=True,exist_ok=False)
        revision=output('git','rev-parse','HEAD')
        manifest={'schema_version':'qt.release_package.v1','revision':revision,
                  'source_tree_hash':output('python3','scripts/provenance/source_tree_hash.py','--git-revision',revision),
                  'run_id':int(os.environ['GITHUB_RUN_ID']),'attempt':int(os.environ['GITHUB_RUN_ATTEMPT']),
                  'images':image_ids(revision)}
        (root/'manifest.json').write_text(json.dumps(manifest,sort_keys=True)+'\n')
    else:
        manifest=json.loads((root/'manifest.json').read_text())
        if manifest['images'] != image_ids(manifest['revision']):
            raise ValueError('qualified image identities changed during proofs')
        archive=root/'images.tar'
        subprocess.run(['docker','save','--output',str(archive),*[v['tag'] for v in manifest['images'].values()]],check=True,timeout=600)
        with archive.open('rb') as stream:
            manifest['archive_sha256']=hashlib.file_digest(stream,'sha256').hexdigest()
        (root/'manifest.json').write_text(json.dumps(manifest,sort_keys=True)+'\n')

if __name__=='__main__':
    main()
