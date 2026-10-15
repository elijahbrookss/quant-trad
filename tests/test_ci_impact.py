from __future__ import annotations
import importlib.util
from pathlib import Path
import subprocess
import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('ci_impact', ROOT/'scripts/ci/ci_impact.py')
impact = importlib.util.module_from_spec(spec)
spec.loader.exec_module(impact)

def test_conservative_union_and_sensitive_boundaries():
    p = impact.plan(['docs/index.md', 'portal/frontend/src/App.jsx', next(iter(impact.PURE))])
    assert {k for k,v in p['required'].items() if v} == {'docs', 'frontend', 'pr-suite'}
    for path in ['src/research_science/check.py', 'portal/backend/service/research/registry.py',
                 'portal/backend/service/research/forward_risk_evaluator.py', 'requirements.lock',
                 'portal/frontend/package-lock.json', 'portal/frontend/src/api/View.jsx',
                 'scripts/db/manual_migration.sql', '.github/workflows/test.yaml', 'unknown.py']:
        assert all(impact.plan(['docs/index.md', path])['required'].values())
    assert all(impact.plan([])['required'].values())
    assert all(impact.plan(['docs/index.md'], full=True)['required'].values())

def test_base_failure_and_release_branches_force_full(monkeypatch):
    monkeypatch.setattr(impact, 'changed', lambda *args: ['docs/index.md'])
    for name, ref in [('push', 'refs/heads/develop'), ('push', 'refs/heads/main'), ('workflow_dispatch', 'refs/heads/feature/x')]:
        assert all(impact.event_plan({'before':'a'*40}, name, ref, 'b'*40)['required'].values())
    monkeypatch.setattr(impact, 'changed', lambda *args: (_ for _ in ()).throw(ValueError('bad base')))
    assert all(impact.event_plan({}, 'push', 'refs/heads/feature/x', 'b'*40)['required'].values())

def test_gate_never_masks_failure_cancellation_or_missing_required():
    plan = impact.plan(['docs/index.md'])
    needs = {job: {'result':'success' if required else 'skipped'} for job,required in plan['required'].items()}
    impact.gate(plan, needs)
    for job in impact.JOBS:
        for outcome in ['failure', 'cancelled', None]:
            with pytest.raises(ValueError):
                impact.gate(plan, {**needs, job:{'result':outcome}})
    with pytest.raises(ValueError):
        impact.gate(plan, {**needs, 'docs':{'result':'skipped'}})

def test_deleted_and_added_paths_both_participate(tmp_path, monkeypatch):
    subprocess.run(['git','init',str(tmp_path)],check=True,capture_output=True)
    monkeypatch.chdir(tmp_path)
    def commit():
        subprocess.run(['git','add','.'],check=True)
        subprocess.run(['git','-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-m','fixture'],check=True,capture_output=True)
        return impact.git('rev-parse','HEAD').decode().strip()
    (tmp_path/'unknown.py').write_text('x=1')
    base=commit()
    (tmp_path/'docs').mkdir()
    (tmp_path/'unknown.py').rename(tmp_path/'docs/index.md')
    paths=impact.changed(base,commit())
    assert set(paths)=={'unknown.py','docs/index.md'}
    assert impact.plan(paths)['full']
