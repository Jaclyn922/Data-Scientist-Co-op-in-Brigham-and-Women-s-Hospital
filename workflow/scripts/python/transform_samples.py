import logging
import pandas as pd
import uuid

GEN3_COLUMNS = ['type','project_id','submitter_id','subjects.submitter_id',
                'biospecimen_anatomic_site','composition','current_weight','days_to_collection',
                'days_to_sample_procurement','diagnosis_pathologically_confirmed','freezing_method',
                'initial_weight','intermediate_dimension','is_ffpe','longest_dimension',
                'method_of_sample_procurement','oct_embedded','preservation_method','sample_type',
                'sample_type_id','shortest_dimension','time_between_clamping_and_freezing',
                'time_between_excision_and_freezing','tissue_type','tumor_code','tumor_code_id','tumor_descriptor']

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
    log = logging.getLogger(__name__)
    logging.basicConfig(level=logging.DEBUG)

    samplemap = pd.read_csv(snakemake.input.sample_map)
    log.debug(f'{samplemap=}')

    cohort = pd.read_csv(snakemake.input.cohort)
    log.debug(f'cohortf=}')
    
    sf = pd.read_csv(snakemake.input.samplefamilies)
    log.debug(f'{sf=}')
    sf = sf.merge(cohort, how='left', left_on='subjectid', right_on='s_subjectid')
    log.debug(f'{sf=}')

    samples = pd.read_csv(snakemake.input.samples)
    log.debug(f'{samples=}')

    samples = samples.merge(samplemap, left_on='s_sampleid', right_on='destsampleid', how='left', indicator=True)
    samples = samples[samples['destsampleid'].isna()]
    log.debug(f'{samples=}')

    samples = samples.merge(sf, how='left', left_on='samplefamilyid', right_on='s_samplefamilyid')
    # samples['S_SUBJECTID'] = samples.apply(fix_subject_id, axis=1)
    log.debug(f'{samples.columns=}')
    samples = samples.rename(columns={
        's_sampleid': 'submitter_id',
        'initialmass': 'initial_weight',
        'S_SUBJECTID': 'subjects.submitter_id',
        'tissuedesc': 'biospecimen_anatomic_site',
    }                             
    )
    log.debug(f'{samples.columns=}')
    samples['project_id'] = 'g0-p0'
    samples['type'] = 'sample'
    samples['sample_type'] = samples['u_initsampletypeid']
    samples['guid'] = samples.apply(lambda x: uuid.uuid4(), axis=1)
    log.debug(f'{samples=}')

    # Add the new columns with 'N/A' as the default value
    for column in GEN3_COLUMNS:
        if column not in samples.columns:
            log.debug(f'{column=} not in {samples.columns=}')
            samples[column] =  None

    # Export the DataFrame as a TSV file
    samples = samples.drop_duplicates('submitter_id',)
    samples.to_csv(snakemake.output.samples, sep='\t', index=False, columns=GEN3_COLUMNS)
