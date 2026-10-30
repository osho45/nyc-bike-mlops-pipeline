"""Read-only live progress; never starts downloads, Spark or model training."""
import argparse,csv,json,time
from datetime import datetime
from pathlib import Path
from jobs.ml_utils import project_root
from tools.ingestion_resources import heartbeat,ResourceStop

def progress(root=None):
    root=Path(root) if root else project_root()/'artifacts/monthly-ingestion-01'
    def read(path,default=None):return json.loads(path.read_text(encoding='utf-8-sig')) if path.exists() else default
    journal=read(root/'controlled-run.json',{});plan=read(root/'plan.json',{});months=[]
    for path in sorted((root/'checkpoints').glob('????-??.json')):
        cp=read(path)
        if cp.get('verified'):
            m=cp['audit']['metrics'];months.append({'month':f"{cp['year']}-{cp['month']:02d}",'split':cp['split'],'source_rows':cp['audit']['reference']['source_rows'],'silver_rows':m['silver_rows'],'quarantine_rows':m['quarantine_rows'],'gold_bi_rows':m['gold_bi_rows'],'gold_ml_rows':m['gold_ml_rows'],'feature_rows_monthly_warmup':m['feature_rows_after_warmup'],'source_bytes':cp['source']['file_size_bytes']})
    years=[read(p) for p in sorted((root/'years').glob('*.json'))] if (root/'years').exists() else []
    blockers=[]
    try:disk=heartbeat(root)
    except ResourceStop as error:disk={};blockers.append(str(error))
    for name in ['resource-stop.json','worker-fatal.json']:
        if (root/name).exists():blockers.append(read(root/name))
    if journal.get('status')=='stopped':blockers.append(journal.get('error'))
    worker={}
    path=root/'worker-memory.csv'
    if path.exists():
        with path.open() as stream:
            for row in csv.DictReader(stream):
                worker['current_bytes']=int(row['current_bytes']);worker['working_set_bytes']=int(row['working_set_bytes']);worker['peak_sampled_working_set_bytes']=max(worker.get('peak_sampled_working_set_bytes',0),int(row['working_set_bytes']))
        worker['heartbeat_age_seconds']=round(time.time()-path.stat().st_mtime,2)
    ready=journal.get('status')=='complete' and {y['year'] for y in years if y.get('verified')}=={2024,2025,2026} and len(months)==len(plan.get('months',[])) and not blockers
    bounded=read(root.parent/'ml-readiness-01'/'metrics.json',{})
    trained=bounded.get('status')=='complete'
    ml_summary={'status':bounded.get('status'),'rows':bounded.get('rows'),'validation_selected_model':bounded.get('validation_selected_model'),'artifacts':'artifacts/ml-readiness-01'} if trained else None
    return {'captured_at':datetime.now().astimezone().isoformat(),'run':journal,'planned_months':len(plan.get('months',[])),'verified_months':months,'verified_years':[y['year'] for y in years if y.get('verified')],'worker_memory':worker,'host_disk':disk,'blockers':blockers,'ready_for_model_training':ready,'model_training_performed':trained,'ingestion_trains_models':False,'bounded_ml_experiment':ml_summary}

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output');args=parser.parse_args();result=progress()
    if args.output:
        target=Path(args.output).resolve()
        if not any(target.is_relative_to(project_root()/folder) for folder in ['docs/audit','artifacts']):raise ValueError('Progress output must remain in audit/artifact folders')
        temporary=target.with_suffix('.tmp');temporary.write_text(json.dumps(result,indent=2)+'\n');temporary.replace(target)
    print(json.dumps(result,indent=2))

if __name__=='__main__':main()
