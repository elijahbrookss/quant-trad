#!/usr/bin/env python3
"""Conservative work-branch job selection; release qualification stays full."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import re
import subprocess

JOBS = ('docs', 'pr-suite', 'frontend', 'deployment-contract',
        'deployment-rehearsal', 'clean-database-bootstrap', 'committed-recovery')
# Reviewed presentation-only files. New/mixed/credential components fail closed.
PRESENTATION = frozenset({
    'portal/frontend/src/index.css',
    'portal/frontend/src/v2/v2.css',
    'portal/frontend/src/v2/v2-operator.css',
    'portal/frontend/src/features/overview/components/ActivityHeatmap.jsx',
    'portal/frontend/src/features/bots/botlens/components/WarningPanel.jsx',
    'portal/frontend/src/features/bots/botlens/components/BotLensHeader.jsx',
})
PURE = frozenset({
    'portal/backend/service/research/event_statistics.py',
    'portal/backend/service/research/directional_model.py',
    'portal/backend/service/research/metrics.py',
    'tests/test_portal/test_event_fact_check.py',
})

def plan(paths, *, full=False, reason='changes'):
    required = {'docs'}
    reasons = []
    for path in paths:
        if not path or path.startswith('/') or '..' in path.split('/'):
            full = True
        elif (path.startswith('docs/') and path.endswith(('.md', '.png', '.jpg', '.svg', '.mmd'))) or path in {'README.md', 'CHANGELOG.md'}:
            pass
        elif path in PRESENTATION:
            required.add('frontend')
        elif path in PURE:
            required.add('pr-suite')
        else:
            full = True
        reasons.append({'path': path, 'full_so_far': full})
    if not paths:
        full = True
        reason = 'empty_or_unavailable_diff'
    return {'schema_version': 'qt.ci_impact.v1', 'full': full, 'reason': reason,
            'paths': list(paths), 'path_decisions': reasons, 'required': {job: full or job in required for job in JOBS}}

def git(*args):
    return subprocess.run(['git', *args], check=True, capture_output=True,
                          timeout=30).stdout

def changed(base, head):
    if not all(re.fullmatch(r'[a-f0-9]{40}', sha or '') and sha != '0'*40 for sha in (base, head)):
        raise ValueError('missing_or_invalid_base')
    git('cat-file', '-e', base+'^{commit}')
    git('cat-file', '-e', head+'^{commit}')
    # No rename collapsing: both deleted source and added destination matter.
    return git('diff', '--name-only', '--no-renames', '-z', base, head).decode('utf-8').rstrip('\0').split('\0')

def event_plan(event, name, ref, head, force=False):
    full = force or ref in ('refs/heads/develop', 'refs/heads/main')
    base = ''
    try:
        if name == 'pull_request':
            base = event['pull_request']['base']['sha']
            pr_head = event['pull_request']['head']['sha']
            base = git('merge-base', base, pr_head).decode().strip()
            head = pr_head
            full = True  # PR-to-release qualification remains complete.
        elif name == 'push':
            if ref.startswith(('refs/heads/feature/', 'refs/heads/hotfix/')):
                # Include all branch contributions, even when an earlier push's
                # full run was cancelled. The baseline is explicit, never inferred
                # from the last pushed commit or an unqualified predecessor.
                baseline = os.environ.get('QT_CI_INTEGRATION_REF', 'origin/develop')
                if not re.fullmatch(r'origin/(?:develop|main|feature/[a-zA-Z0-9._/-]+)', baseline):
                    raise ValueError('invalid_integration_baseline')
                base = git('merge-base', baseline, head).decode().strip()
            else:
                base = event['before']
                full = True
        elif name == 'workflow_dispatch':
            full = True
        else:
            full = True
        result = plan(changed(base, head), full=full)
    except (KeyError, ValueError, UnicodeError, subprocess.SubprocessError):
        result = plan([], full=True, reason='base_or_diff_unavailable')
    result.update(base=base, head=head)
    return result

def gate(selection, needs):
    if selection.get('schema_version') != 'qt.ci_impact.v1' or set(selection.get('required', {})) != set(JOBS):
        raise ValueError('invalid_selection')
    for job, required in selection['required'].items():
        if type(required) is not bool:
            raise ValueError('invalid_requirement')
        result = needs.get(job, {}).get('result')
        if result != 'success' and (required or result != 'skipped'):
            raise ValueError(f'CI gate refused {job}: required={required} result={result}')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--gate', action='store_true')
    args = parser.parse_args()
    if args.gate:
        needs = json.loads(os.environ['QT_CI_NEEDS'])
        if needs.get('changes', {}).get('result') != 'success':
            raise ValueError('selection_job_failed')
        gate(json.loads(needs['changes']['outputs']['plan']), needs)
        print('All computed required checks succeeded')
    else:
        event = json.loads(Path(os.environ['GITHUB_EVENT_PATH']).read_text())
        result = event_plan(event, os.environ['GITHUB_EVENT_NAME'], os.environ['GITHUB_REF'],
                            os.environ['GITHUB_SHA'], event.get('inputs', {}).get('force_full') in (True, 'true'))
        print(json.dumps(result, indent=2))
        with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
            out.write('plan='+json.dumps(result, separators=(',', ':'))+'\n')
            for job, required in result['required'].items():
                out.write(job+'='+str(required).lower()+'\n')

if __name__ == '__main__':
    main()
