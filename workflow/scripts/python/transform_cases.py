import logging
import pandas as pd
import uuid

GEN3_COLUMNS = ['type', 'project_id', 'submitter_id', 'experiments.submitter_id','consent_codes','disease_type','primary_site']

GEN3_TYPE = 'subject'
PROJECT_CODE = 'p0'
PROJECT_ID = 'g0-p0'

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
    if not 'snakemake' in locals():snakemake = None
    
    subjects = pd.read_csv(snakemake.input.subjects)
    subjects['S_SUBJECTID'] = subjects.apply(fix_subject_id, axis=1)
    subjects = subjects.rename(columns={
        'S_SUBJECTID': 'submitter_id',
        'dataset_name': 'experiments.submitter_id',
    }
    )
    subjects['type'] = GEN3_TYPE
    subjects['project_id'] = PROJECT_ID
    subjects['guid'] = subjects.apply(lambda x: uuid.uuid4(), axis=1)

    # Add any undefined columns with 'N/A' as the default value
    for column in GEN3_COLUMNS:
        if column not in subjects.columns:
            subjects[column] = None

    # Export the DataFrame as a TSV file
    subjects.to_csv(snakemake.output.subjects, index=False, columns=GEN3_COLUMNS)
