#!/usr/bin/env python3
"""Private routine configuration identity; never print resolved configuration."""
from __future__ import annotations
import argparse,hashlib,json,os,stat,subprocess,sys,tempfile
from pathlib import Path

DATA_TARGETS={'/app/logs/market-structure','/host-docker','/host-archive','/run/qt-host-udev','/qt-history','/var/lib/postgresql/data'}
LIMIT=32*1024*1024

def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()

def file_digest(path):
    if path.stat().st_size>LIMIT:raise ValueError('configuration file too large')
    return hashlib.sha256(path.read_bytes()).hexdigest()

def normalized(model,revision,source_hash):
    model=json.loads(json.dumps(model))
    for key in list(model):
        if key.startswith('x-'):model.pop(key)
    for service in model['services'].values():
        for key in ('image','build','pull_policy'):service.pop(key,None)
        env=service.get('environment',{})
        for key,expected in [('SOURCE_REVISION',revision),('SOURCE_TREE_HASH',source_hash),
                             ('QT_BOT_RUNTIME_IMAGE','quanttrad-backend:'+revision)]:
            if env.get(key)==expected:env[key]='<qualified-source>'
    return model

def binding_identity(model,env_file):
    if env_file.is_symlink() or not env_file.is_file() or env_file.stat().st_mode&0o077:
        raise ValueError('operator environment must be private regular file')
    bindings={'operator_environment':file_digest(env_file)}
    count=0;total=0
    for name,service in sorted(model['services'].items()):
        for mount in service.get('volumes',[]):
            if mount.get('type')!='bind':continue
            source=Path(mount['source']);target=mount['target'];resolved=source.resolve(strict=True)
            meta=source.stat()
            entry={'resolved':str(resolved),'mode':stat.S_IMODE(meta.st_mode),'uid':meta.st_uid,'gid':meta.st_gid}
            if source.is_file():entry.update(kind='file',sha256=file_digest(source))
            elif source.is_dir():
                entry['kind']='directory'
                if target not in DATA_TARGETS:
                    files={}
                    pending=[source]
                    while pending:
                        for item in pending.pop().iterdir():
                            count+=1
                            if count>2000:raise ValueError('configuration tree exceeds bound')
                            if item.is_symlink():raise ValueError('configuration tree symlink')
                            child=item.stat()
                            value={'mode':stat.S_IMODE(child.st_mode),'uid':child.st_uid,'gid':child.st_gid}
                            if item.is_dir():value['kind']='directory';pending.append(item)
                            elif item.is_file():
                                total+=child.st_size
                                if total>LIMIT:raise ValueError('configuration tree exceeds bound')
                                value.update(kind='file',sha256=file_digest(item))
                            else:raise ValueError('unknown configuration tree entry')
                            files[str(item.relative_to(source))]=value
                    entry['files']=files
            elif stat.S_ISSOCK(meta.st_mode):entry['kind']='socket'
            else:raise ValueError('unknown configuration binding')
            bindings[name+':'+target]=entry
    return digest(bindings)

def output(*args):
    return subprocess.check_output(args,text=True,timeout=30).strip()

def running_identity(model):
    identity={}
    for name in sorted(model['services']):
        ids=output('docker','ps','--all','--quiet','--filter','label=com.docker.compose.project='+model['name'],
                   '--filter','label=com.docker.compose.service='+name,'--filter','label=com.docker.compose.oneoff=False').splitlines()
        if len(ids)!=1:raise ValueError('current runtime inventory differs')
        value=json.loads(output('docker','inspect',ids[0]))[0]
        identity[name]={key:value[key] for key in ('Id','Image','Config','HostConfig','Mounts')}
    return digest(identity)

def configuration_identity(model,revision,source_hash,env_file,*,running=True):
    if not 1<=len(model.get('services',{}))<=64:raise ValueError('configuration service inventory exceeds bound')
    for key in ('configs','secrets'):
        if model.get(key) or any(service.get(key) for service in model['services'].values()):
            raise ValueError('routine configuration uses unsupported Compose configs/secrets')
    result={'configuration_sha256':digest(normalized(model,revision,source_hash)),
            'bindings_sha256':binding_identity(model,env_file)}
    if running:result['running_sha256']=running_identity(model)
    return result

def admit(baseline,identity,previous):
    if baseline.get('schema_version')!='qt.routine_configuration.v1' or baseline.get('revision')!=previous:
        raise ValueError('approved routine configuration baseline absent')
    if any(baseline.get(key)!=value for key,value in identity.items()):
        raise ValueError('routine configuration drift; reviewed manual deployment required')

def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['capture','admit'])
    p.add_argument('--state',type=Path,required=True);p.add_argument('--env-file',type=Path,required=True)
    p.add_argument('--revision',required=True);p.add_argument('--source-hash',required=True)
    p.add_argument('--previous');p.add_argument('--without-running',action='store_true');args=p.parse_args()
    raw=sys.stdin.read(LIMIT+1)
    if len(raw)>LIMIT:raise ValueError('configuration model exceeds bound')
    identity=configuration_identity(json.loads(raw),args.revision,args.source_hash,args.env_file,running=not args.without_running)
    path=args.state/'routine-configuration.json'
    if args.action=='capture':
        baseline={'schema_version':'qt.routine_configuration.v1','revision':args.revision,**identity}
        fd,temp=tempfile.mkstemp(prefix='routine-configuration.',dir=args.state)
        try:
            with os.fdopen(fd,'w') as stream:json.dump(baseline,stream);stream.write('\n')
            os.replace(temp,path)
        finally:
            if os.path.exists(temp):os.unlink(temp)
    else:
        if path.is_symlink() or not path.is_file() or path.stat().st_mode&0o077:
            raise ValueError('private routine configuration baseline absent')
        admit(json.loads(path.read_text()),identity,args.previous)

if __name__=='__main__':
    try:main()
    except Exception:raise SystemExit('routine configuration admission failed; preserve state and use reviewed manual reconciliation') from None
