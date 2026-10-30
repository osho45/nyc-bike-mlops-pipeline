"""Explicit, resumable source/data transfer manifests. Never trains or downloads."""
import argparse,ast,hashlib,json,os,shutil,sys
from pathlib import Path,PurePosixPath

BASE=Path('artifacts/native-network-capacity-03/384')
GOLD=Path('artifacts/monthly-ingestion-01/warehouse/gold/station_hourly_observations/data')

def sha(path):
 h=hashlib.sha256()
 with Path(path).open('rb') as f:
  for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
 return h.hexdigest()

def safe_path(root,relative):
 p=PurePosixPath(relative)
 if not p.parts or p.is_absolute() or '\\' in relative or ':' in relative or '..' in p.parts:raise ValueError('Unsafe transfer path')
 root=Path(root).resolve();target=root.joinpath(*p.parts)
 if not target.resolve().is_relative_to(root):raise ValueError('Transfer path escapes root')
 return target

def source_closure(root,seeds):
 pending=list(seeds);seen=set()
 while pending:
  relative=pending.pop()
  if relative in seen:continue
  file=safe_path(root,relative)
  if not file.is_file():raise FileNotFoundError(relative)
  seen.add(relative)
  if file.suffix!='.py':continue
  for node in ast.walk(ast.parse(file.read_text(encoding='utf-8-sig'))):
   names=[node.module] if isinstance(node,ast.ImportFrom) and node.module else [x.name for x in node.names] if isinstance(node,ast.Import) else []
   for name in names:
    if name.split('.')[0] not in ['jobs','tools']:continue
    local=name.replace('.','/')+'.py'
    if safe_path(root,local).is_file():pending.append(local)
 return seen

def development_inventory(root):
 """Read only 24 explicit development partitions; reject ambiguous versions."""
 import pyarrow.parquet as pq
 import pandas as pd
 population=pd.read_parquet(Path(root)/'artifacts/cohort-48-01/development_population_profiles.parquet')
 if set(population.target_year)!={2024,2025}:raise ValueError('Population must be development-only')
 files=[];total=0
 for year in [2024,2025]:
  for month in range(1,13):
   checkpoint=Path(f'artifacts/monthly-ingestion-01/checkpoints/{year}-{month:02d}.json');record=json.loads((Path(root)/checkpoint).read_text())
   if record['phase']!='verified' or record['year']!=year or record['month']!=month:raise ValueError('Unverified development checkpoint')
   paths=list((Path(root)/GOLD/f'target_year={year}'/f'target_month={month}').glob('*.parquet'))
   if len(paths)!=1:raise ValueError('Ambiguous partition versions: use an explicitly verified snapshot, never glob duplicate versions')
   path=paths[0];pf=pq.ParquetFile(path);expected=int(population.loc[(population.target_year==year)&(population.target_month==month),'hours'].sum())
   if pf.metadata.num_rows!=expected:raise ValueError('Development profile/partition row mismatch')
   expected_fields={'target_year':year,'target_month':month,'source_checksum':record['source']['sha256'],'source_revision':record['source']['source_revision']}
   if not {'station_id','station_hour_ts','ride_count',*expected_fields}.issubset(pf.schema_arrow.names):raise ValueError('Gold ML schema mismatch')
   for group in range(pf.metadata.num_row_groups):
    for name,value in expected_fields.items():
     stat=pf.metadata.row_group(group).column(pf.schema_arrow.names.index(name)).statistics
     if stat is None or stat.null_count!=0 or stat.min!=value or stat.max!=value:raise ValueError('Partition year/month/source-version mismatch')
   relative=path.relative_to(Path(root)).as_posix();files.append((relative,{'year':year,'month':month,'rows':expected,'source_checksum':record['source']['sha256']}));files.append((checkpoint.as_posix(),{'year':year,'month':month,'kind':'verified_checkpoint'}));total+=expected
 if total!=38820660:raise ValueError('Development source count changed; audit before transfer')
 return files,total

def plan(root,include_development=False):
 root=Path(root).resolve()
 seeds=['tools/native_4g_worker.py','tools/native_4g_preflight.py','tools/training_transfer.py']
 source=source_closure(root,seeds)
 source.update(['docker-compose.native-4g.yml','config/native_training_4g.yml','config/native_network_capacity.yml','config/network_scaling_plan.yml','config/native_runtime_lock.json','airflow-image/Dockerfile','airflow-image/requirements.txt'])
 for relative in ['docs/TRAINING_HOST_TRANSFER.md']:
  if (root/relative).is_file():source.add(relative)
 categories={relative:('source',{}) for relative in source}
 for folder in ['matrices/train','matrices/validation','native','checkpoints']:
  for file in (root/BASE/folder).iterdir():
   if file.is_file():categories[file.relative_to(root).as_posix()]=('benchmark',{})
 for name in ['prepare/result.json','construct/result.json','fit/result.json','export/result.json','experiment.json','matrix_manifests.json','capacity_model.txt','training_curves.json','train_predictions.parquet','validation_predictions.parquet','feature_audit.json','retention_manifest.json','software.json','code_sha256.json']:
  categories[(BASE/name).as_posix()]=('benchmark',{})
 total_rows=None
 if include_development:
  development,total_rows=development_inventory(root)
  population_sha=sha(root/'artifacts/cohort-48-01/development_population_profiles.parquet')
  for label,count in [('384',384),('all_eligible',1898)]:
   cohort=json.loads((root/f'artifacts/network-scaling-prep-02/stage_plans/{label}.json').read_text())
   ids=cohort['stations']
   if cohort['development_source_sha256']!=population_sha or ids!=sorted(set(ids)) or len(ids)!=count or cohort['prospective_station_codes']!={station:i for i,station in enumerate(ids)} or cohort['training_enabled']:
    raise ValueError('Frozen development cohort/source/disablement mismatch')
  for relative,metadata in development:categories[relative]=('development',metadata)
  for relative in ['artifacts/cohort-48-01/development_population_profiles.parquet','artifacts/network-scaling-prep-02/stage_plans/384.json','artifacts/network-scaling-prep-02/stage_plans/all_eligible.json','artifacts/network-scaling-prep-02/annual_gate_evidence.json','artifacts/network-scaling-prep-02/eligibility_exclusions.json']:
   categories[relative]=('development',{})
 records=[]
 for relative,(category,metadata) in sorted(categories.items()):
  path=safe_path(root,relative)
  if path.is_symlink() or not path.is_file():raise ValueError(f'Only regular explicit source files allowed: {relative}')
  records.append({'path':relative,'category':category,'bytes':path.stat().st_size,'sha256':sha(path),**metadata})
 totals={category:sum(x['bytes'] for x in records if x['category']==category) for category in ['source','benchmark','development']}
 return {'schema_version':1,'snapshot':'current working tree, including uncommitted source; no Git history','allowed_years':[2024,2025],'files':records,'bytes_by_category':totals,'total_bytes':sum(totals.values()),'development_rows':total_rows,'image_payload_included':False,'full_network_training_enabled':False,'excluded':['credentials','Git metadata','virtualenvs','live leases/heartbeats','2026 data/outcomes','raw ZIP/CSV','Bronze/Silver/BI','unrelated models/caches'],'runtime_image_id':'sha256:809353853ccd99ce12d5a4f359780f025f4932ac46fce3f065c8f57477c85c18'}

def verify(root,manifest,categories=None):
 for record in manifest['files']:
  if categories and record['category'] not in categories:continue
  target=safe_path(root,record['path'])
  if target.is_symlink() or not target.is_file() or target.stat().st_size!=record['bytes'] or sha(target)!=record['sha256']:raise ValueError(f"Transfer integrity mismatch: {record['path']}")
 return True

def copy_files(source,destination,manifest,categories=None):
 copied=skipped=resumed=0
 for record in manifest['files']:
  if categories and record['category'] not in categories:continue
  src=safe_path(source,record['path']);dst=safe_path(destination,record['path'])
  if src.is_symlink() or src.stat().st_size!=record['bytes'] or sha(src)!=record['sha256']:raise ValueError('Source changed since manifest')
  if dst.exists():
   if dst.is_symlink() or sha(dst)!=record['sha256']:raise FileExistsError(f'Conflicting destination preserved: {dst}')
   skipped+=1;continue
  dst.parent.mkdir(parents=True,exist_ok=True);partial=dst.with_name(dst.name+'.transfer-part')
  if partial.is_symlink():raise ValueError('Partial transfer cannot be a symlink')
  size=partial.stat().st_size if partial.exists() else 0
  if size>record['bytes']:raise ValueError('Partial file is larger than source')
  with src.open('rb') as inp:
   if size:
    with partial.open('rb') as old:
     remaining=size
     while remaining:
      count=min(1024*1024,remaining)
      if inp.read(count)!=old.read(count):raise ValueError('Partial prefix differs; preserve and inspect it')
      remaining-=count
    resumed+=1
   with partial.open('ab') as out:
    shutil.copyfileobj(inp,out,length=1024*1024);out.flush();os.fsync(out.fileno())
  if sha(partial)!=record['sha256']:raise ValueError('Copied data failed hash; partial preserved')
  if dst.exists():raise FileExistsError('Destination appeared during copy; preserve both files')
  if os.name=='nt':os.rename(partial,dst)
  else:
   os.link(partial,dst);partial.unlink()
  copied+=1
 return {'copied':copied,'skipped_identical':skipped,'resumed':resumed}

def full_network_plan(root,manifest,benchmark_root=None):
 verify(root,manifest)
 reasons=['Full-network training is explicitly disabled; this command never trains','Existing isolated worker supports 384 stations only; an explicitly authorized 1898 executor/generalized feature loader is still required']
 if manifest.get('development_rows')!=38820660:reasons.append('Verified 2024/2025 Gold source snapshot is not included')
 required=['construct','compact/fit','compact/export','deferred/fit','deferred/curves','deferred/export']
 receipts=[]
 if benchmark_root:
  from jobs.native_4g_resources import phase_admission
  for phase in required:
   file=Path(benchmark_root)/phase/'result.json'
   if not file.exists():reasons.append(f'Missing bounded 4 GiB receipt: {phase}');continue
   phase_admission(json.loads(file.read_text()));receipts.append(phase)
  if len(receipts)==len(required):
   benchmark=Path(benchmark_root)
   for name in ['train.bin','validation.bin']:
    if sha(benchmark/'native'/name)!=sha(Path(root)/BASE/'native'/name):raise ValueError('Bounded benchmark native hash mismatch')
   for variant in ['compact','deferred']:
    for relative in ['capacity_model.txt','train_predictions.parquet','validation_predictions.parquet',*[f'checkpoints/iteration-{n:03d}.txt' for n in [20,40,60,80,100]]]:
     if sha(benchmark/variant/relative)!=sha(Path(root)/BASE/relative):raise ValueError('Bounded model/checkpoint/forecast hash mismatch')
   provenance=json.loads((benchmark/'provenance.json').read_text())
   for relative,h in provenance['code_sha256'].items():
    if sha(safe_path(root,relative))!=h:raise ValueError('Bounded benchmark code/config provenance mismatch')
 else:reasons.append('No real bounded 4 GiB benchmark receipts supplied')
 return {'status':'blocked','stations':1898,'training_year':2024,'validation_year':2025,'expected_train_rows':16353168,'expected_validation_rows':16626480,'verified_benchmark_receipts':receipts,'blockers':reasons,'full_network_training_enabled':False,'model_invoked':False}

def main():
 p=argparse.ArgumentParser();sub=p.add_subparsers(dest='action',required=True)
 a=sub.add_parser('plan');a.add_argument('--root',type=Path,default=Path('.'));a.add_argument('--output',type=Path,required=True);a.add_argument('--include-development',action='store_true')
 for action in ['verify','copy','full-network-plan']:
  a=sub.add_parser(action);a.add_argument('--root',type=Path,default=Path('.'));a.add_argument('--manifest',type=Path,required=True)
  if action in ['verify','copy']:a.add_argument('--category',action='append',choices=['source','benchmark','development'])
  if action=='copy':a.add_argument('--destination',type=Path,required=True)
  if action=='full-network-plan':a.add_argument('--benchmark-root',type=Path)
 a=p.parse_args()
 if a.action=='plan':
  result=plan(a.root,a.include_development);a.output.parent.mkdir(parents=True,exist_ok=True)
  if a.output.exists():raise FileExistsError('Choose a new immutable manifest filename')
  a.output.write_text(json.dumps(result,indent=2)+'\n');a.output.with_suffix('.files.txt').write_text('\n'.join(x['path'] for x in result['files'])+'\n');print(json.dumps({'files':len(result['files']),'bytes':result['total_bytes'],'categories':result['bytes_by_category']}));return 0
 manifest=json.loads(a.manifest.read_text())
 if a.action=='verify':verify(a.root,manifest,a.category);print('Verified all selected SHA-256 hashes');return 0
 if a.action=='copy':print(json.dumps(copy_files(a.root,a.destination,manifest,a.category)));return 0
 print(json.dumps(full_network_plan(a.root,manifest,a.benchmark_root),indent=2));return 2

if __name__=='__main__':raise SystemExit(main())
