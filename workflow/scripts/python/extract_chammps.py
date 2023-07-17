import pandas as pd
from pathlib import Path

def fix_subject_id(row):
    sid = row['S_SUBJECTID']
    sname = row['Sample_Name']
    if str(sid).startswith('ST-'):
        stid = sid
    elif str(sname).startswith('ST-'):
        stid = sname
    elif sid:
        stid = sid
    else:
        stid = sname
    return stid

if __name__ == '__main__':
    def annotate(ds):
        dsid = ds['id']
        dsurl = ds["url"]
        if pd.notna(dsurl):
            dsurl = dsurl.replace('file://', '')
        dsname = ds["subject"]

        ds['redmine_url'] = f'https://chanmine.bwh.harvard.edu/issues/{dsid}'

        try:
            sample_manifest_path = Path(dsurl) / '.chammps/sample-manifest.csv'
            ds_samples = pd.read_csv(sample_manifest_path)
            S = len(ds_samples)
        except:
            sample_manifest_path = None
            ds_samples = pd.DataFrame({'S_SAMPLEID': []})
            S = 0

        try:
            subject_manifest_path = Path(dsurl) / '.chammps/subject-manifest.csv'
            ds_subjects = pd.read_csv(subject_manifest_path)
            N = len(ds_subjects)
        except:
            subject_manifest_path = None
            ds_subjects = pd.DataFrame()
            N = 0

        if 'S_SAMPLEID' in ds_samples.columns:
            ds_samples = ds_samples.merge(samples, left_on='S_SAMPLEID', right_on='s_sampleid', how='left')
            ds_samples['dataset_id'] = dsid
            ds_samples['dataset_name'] = dsname

        sample_batches.append(ds_samples)

        ds['S'] = S
        ds['N'] = N
        ds['sample-manifest'] = sample_manifest_path

        return ds

    datasets = pd.read_csv(snakemake.input.datasets)
    samples = pd.read_csv(snakemake.input.samples)
    sample_batches = []
    datasets = datasets.apply(annotate, axis=1)

    
    if sample_batches:
        dataset_samples = pd.concat(sample_batches)
    else:
        dataset_samples = pd.DataFrame()

    def agg_unique(items):
        s = list(set(list(items)))
        if len(s) == 1:
            return s[0]
        return s
        
    dataset_samples['S_SUBJECTID'] = dataset_samples.apply(fix_subject_id, axis=1)
    dataset_subjects = dataset_samples.groupby(['S_SUBJECTID', 'dataset_id']).agg(agg_unique)
        
    datasets.to_csv(snakemake.output.datasets)
    dataset_subjects.to_csv(snakemake.output.subjects)
    dataset_samples.to_csv(snakemake.output.samples)
