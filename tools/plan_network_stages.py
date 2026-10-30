"""Write development-only future cohort manifests. Never invokes a model."""
import argparse,json
from pathlib import Path
import pandas as pd
from jobs.cohort_selection import eligible_stations,select_development_cohort
from jobs.scaling import digest


def main():
    p=argparse.ArgumentParser();p.add_argument('--population',type=Path,default=Path('artifacts/cohort-48-01/development_population_profiles.parquet'));p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    monthly=pd.read_parquet(a.population)
    if not set(monthly.target_year).issubset({2024,2025}):raise ValueError('Development years only')
    plans=[]
    for count in [96,192,384]:
        cohort,coverage=select_development_cohort(monthly,count);plans.append((str(count),cohort,coverage))
    population=eligible_stations(monthly);full=population.to_dict('records');plans.append(('all_eligible',full,{'selected_stations':len(full)}))
    for label,cohort,coverage in plans:
        ids=sorted(x['station_id'] for x in cohort);manifest={'status':'planning_only_not_model_compatible','development_source_sha256':digest(a.population),'stations':ids,'prospective_station_codes':{s:i for i,s in enumerate(ids)},'coverage':coverage,'expected_2024_feature_rows':len(ids)*8616,'expected_2025_rows':len(ids)*8760,'guard_checks':len(ids)*5,'training_enabled':False,'inference_enabled':False,'reuse_old_station_code_model':False}
        (a.output/f'{label}.json').write_text(json.dumps(manifest,indent=2)+'\n')
    print(json.dumps({label:len(cohort) for label,cohort,_ in plans}))

if __name__=='__main__':main()
