"""Present development-only capacity measurements; no ML execution."""
import argparse,json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def main():
    p=argparse.ArgumentParser();p.add_argument('--run',type=Path,default=Path('artifacts/network-scaling-prep-02'));a=p.parse_args();m=json.loads((a.run/'metrics.json').read_text());c=m['capacity'];rows=m['eligible_feature_rows'];bpr=c['dataframe_bytes_per_row_assumption']
    labels=['Training float32 X only','Train + validation numeric X/y','One training pandas frame','Train + validation pandas frames'];values=[c['float32_train_matrix_bytes'],c['float32_train_and_validation_features_labels_bytes'],c['estimated_one_train_dataframe_bytes'],(rows['train']+rows['validation'])*bpr]
    plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False});fig,ax=plt.subplots(figsize=(10,4),layout='constrained');bars=ax.barh(labels,[v/1024**3 for v in values],color=['#2563eb','#2563eb','#dc2626','#dc2626']);ax.invert_yaxis();ax.axvline(2,color='#111827',linestyle='--',label='Existing 2 GiB container cap');ax.set(xlabel='GiB — separate storage scenarios, not measured training peaks',title='1,898-station development workload | native model memory excluded',xlim=(0,max(values)/1024**3+1));ax.legend(loc='lower right');ax.grid(axis='x',alpha=.18);ax.set_axisbelow(True)
    for bar,v in zip(bars,values):ax.text(v/1024**3+.03,bar.get_y()+bar.get_height()/2,f'{v/1024**3:.3f}',va='center')
    target=Path('docs/images/network_scaling_memory.png');target.parent.mkdir(parents=True,exist_ok=True);fig.savefig(target,dpi=150);plt.close(fig)
    summary={**m,'eligibility_exclusions':json.loads((a.run/'eligibility_exclusions.json').read_text()),'annual_gate_evidence':json.loads((a.run/'annual_gate_evidence.json').read_text()),'estimates_are_not_full_network_training_measurements':True}
    target=Path('docs/audit/network_scaling.json');target.parent.mkdir(parents=True,exist_ok=True);target.write_text(json.dumps(summary,indent=2)+'\n')

if __name__=='__main__':main()
