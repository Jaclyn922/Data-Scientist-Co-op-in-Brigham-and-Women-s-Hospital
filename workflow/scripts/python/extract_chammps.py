import pandas as pd
from pathlib import Path

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker

DBURL = 'oracle://sapphire_r:sapphire_r0@oar1.bwh.harvard.edu:1521/labvprd'

sapphire8_md = MetaData(schema='SAPPHIRE8')
engine = create_engine(DBURL)
Session = sessionmaker(bind=engine)
session = Session()

Sample = Table('S_SAMPLE', sapphire8_md, autoload=True, autoload_with=engine)
SampleFamily = Table('S_SAMPLEFAMILY', sapphire8_md, autoload=True, autoload_with=engine)

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
            ds_samples['experiments.submitter_id'] = dsname

        sample_batches.append(ds_samples)

        ds['S'] = S
        ds['N'] = N
        ds['sample-manifest'] = sample_manifest_path

        return ds

    trackers = pd.read_csv('tmp/datasets.csv')
    sample_batches = []
    datasets = datasets.apply(annotate, axis=1)
   
    if sample_batches:
        samples = pd.concat(sample_batches)
    else:
        samples = pd.DataFrame()

    datasets = datasets.merge(samples, on='id', how='left')

    chammps.to_csv(snakemake.output.df)