"""Year-gated coordinator shared by the scheduled task and bounded dry run."""
import json,time
from pathlib import Path
from jobs.monthly_archives import atomic_json
from tools.monthly_ingestion import ROOT,inventory,execute_month,activate,api,PREFIX,STAGES
from tools.ingestion_resources import check,ResourceStop

def ordered_run(items,execute,year_gate,resource_check):
    ordered=sorted(items,key=lambda x:(x['year'],x['month']))
    if len({(x['year'],x['month']) for x in ordered})!=len(ordered):raise ValueError('Duplicate plan months')
    current=None
    for item in ordered:
        if current is not None and current!=item['year']:year_gate(current)
        resource_check(item);execute(item);current=item['year']
    if current is not None:year_gate(current)

def bounded_dry_run(run_id):
    root=ROOT/'coordinator-checks'/run_id.replace('/','_');root.mkdir(parents=True,exist_ok=True)
    prior_checkpoint=root/'recovery-checkpoint.json';restored_prior=prior_checkpoint.exists()
    if restored_prior:
        prior=json.loads(prior_checkpoint.read_text());assert prior['pending']==[2024,3] and prior['verified']==[[2024,1],[2024,2]]
    items=[{'year':y,'month':m} for y in [2024,2025] for m in range(1,13)][:14]
    events=[];verified={(2024,1),(2024,2)}
    def execute(item):
        key=(item['year'],item['month']);events.append(['skip' if key in verified else 'execute',*key])
        if key==(2024,3):raise RuntimeError('Simulated interruption before March commit')
    try:ordered_run(list(reversed(items)),execute,lambda y:events.append(['year',y]),lambda i:None)
    except RuntimeError:pass
    assert events==[['skip',2024,1],['skip',2024,2],['execute',2024,3]]
    atomic_json(root/'recovery-checkpoint.json',{'verified':sorted(map(list,verified)),'pending':[2024,3]})
    restored=json.loads((root/'recovery-checkpoint.json').read_text());verified={tuple(k) for k in restored['verified']};resumed=[]
    def resume(item):
        key=(item['year'],item['month']);resumed.append(['skip' if key in verified else 'execute',*key]);verified.add(key)
    def year_gate(year):
        assert all((year,m) in verified for m in range(1,13)) if year==2024 else True
        resumed.append(['year',year])
    ordered_run(items,resume,year_gate,lambda i:None)
    assert resumed.index(['year',2024])<resumed.index(['execute',2025,1])
    blocked=[]
    def fail_year(year):raise ValueError('Simulated failed year audit')
    try:ordered_run(items,lambda i:blocked.append((i['year'],i['month'])),fail_year,lambda i:None)
    except ValueError:pass
    assert not any(y==2025 for y,m in blocked)
    resource_events=[]
    try:ordered_run(items,lambda i:resource_events.append(i),lambda y:None,lambda i:(_ for _ in ()).throw(ResourceStop('Simulated disk stop')))
    except ResourceStop:pass
    assert not resource_events
    result={'verified':True,'simulation_only':True,'restored_checkpoint_from_prior_task_attempt':restored_prior,'real_downloads':0,'real_asset_events':0,'real_table_writes':0,'initial_interruption_trace':events,'resume_trace':resumed,'year_failure_blocks_next_year':True,'resource_failure_prevents_execution':True,'real_february_checkpoint_verified':json.loads((ROOT/'checkpoints/2024-02.json').read_text())['verified']}
    atomic_json(root/'result.json',result);return result

def run():
    from tools.full_month_queue import FileQueueRunner
    from jobs.source_versions import month_lock
    if not json.loads((ROOT/'pilot-complete.json').read_text()).get('verified'):raise ValueError('Pilot gate missing')
    preflight=ROOT/'coordinator-preflight.json'
    if not preflight.exists() or not json.loads(preflight.read_text()).get('verified'):raise ValueError('Scheduled coordinator dry run must pass first')
    items=inventory()
    if any((year,month) not in {(x['year'],x['month']) for x in items} for year in [2024,2025] for month in range(1,13)):raise ValueError('Required complete-year sources unavailable')
    run_id='controlled-'+str(time.time_ns());journal={'run_id':run_id,'started_at':time.time(),'status':'running','verified_months':[],'model_training':False}
    atomic_json(ROOT/'controlled-run.json',journal)
    def execute(item):
        key=f"{item['year']}-{item['month']:02d}";started=time.monotonic();job_ids={p.name for p in (ROOT/'queue').glob('*.result.json')}
        if key=='2024-01' and not (ROOT/'checkpoints/2024-01.json').exists():
            result=FileQueueRunner(ROOT,'adopt',run_id,1)(['python','-m','tools.monthly_adopt','--config',str(ROOT/'config.yml'),'--year','2024','--month','1'])
            if result.returncode:raise RuntimeError(result.stderr[-3000:])
        result=execute_month(item)
        jobs=[json.loads(p.read_text()) for p in (ROOT/'queue').glob('*.result.json') if p.name not in job_ids]
        metric={'month':key,'completed_at':time.time(),'seconds_this_attempt':round(time.monotonic()-started,3),'source_bytes':result['source']['file_size_bytes'],'download_bytes_this_attempt':result['source'].get('bytes_transferred_this_call',0) if jobs else 0,'data_reused':not jobs or result.get('reuse_method') is not None,'spark_jobs':len(jobs),'peak_sampled_working_set_bytes':max([j['memory']['peak_sampled_working_set_bytes'] for j in jobs],default=0),'quarantine_rows':result['audit']['metrics']['quarantine_rows'],'silver_rows':result['audit']['metrics']['silver_rows'],'feature_invalid_counts':result['audit']['metrics']['feature_invalid_counts']}
        with (ROOT/'monthly-metrics.jsonl').open('a') as stream:stream.write(json.dumps(metric)+'\n')
        journal['verified_months'].append(key);journal['last_verified']=key;atomic_json(ROOT/'controlled-run.json',journal)
    def year_gate(year):
        check(ROOT)
        path=ROOT/'years'/f'{year}.json';expected_months=sorted(i['month'] for i in items if i['year']==year)
        if path.exists():
            old=json.loads(path.read_text())
            hashes={str(m):json.loads((ROOT/'checkpoints'/f'{year}-{m:02d}.json').read_text())['source']['sha256'] for m in expected_months}
            if old.get('verified') and old['months']==expected_months and old.get('source_sha256')==hashes:
                journal.setdefault('verified_years',[]).append(year);atomic_json(ROOT/'controlled-run.json',journal);return
        result=FileQueueRunner(ROOT,'year-audit',run_id+str(year),1)(['python','-m','tools.year_audit','--config',str(ROOT/'config.yml'),'--year',str(year)])
        if result.returncode:raise RuntimeError('Year gate failed: '+result.stderr[-3000:])
        if not json.loads((ROOT/'years'/f'{year}.json').read_text())['verified']:raise ValueError('Year audit did not verify')
        journal.setdefault('verified_years',[]).append(year);atomic_json(ROOT/'controlled-run.json',journal)
    activate()
    def resources(item):
        journal['next_month']=f"{item['year']}-{item['month']:02d}";atomic_json(ROOT/'controlled-run.json',journal);check(ROOT,item['bytes'])
    try:
        with month_lock(ROOT/'controller-locks',2013,1):ordered_run(items,execute,year_gate,resources)
        journal['status']='complete'
    except Exception as error:
        journal.update(status='stopped',error=repr(error),stopped_at=time.time());activate(paused=True);raise
    finally:
        journal['updated_at']=time.time();atomic_json(ROOT/'controlled-run.json',journal)
