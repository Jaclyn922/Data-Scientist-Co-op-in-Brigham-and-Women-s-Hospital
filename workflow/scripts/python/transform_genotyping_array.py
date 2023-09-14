import hashlib
import logging
import pandas as pd
from pathlib import Path
import uuid

GEN3_TYPE = 'aggregated_genotyping_array'
PROJECT_CODE = 'p0'
PROJECT_ID = 'g0-p0'
GEN3_COLUMNS = [
    'type', 'project_id', 'submitter_id', 'aliquots.submitter_id', 'core_metadata_collections.submitter_id',
    'genotyping_arrays.submitter_id','data_category', 'data_format', 'data_type', 'file_name', 'file_size', 'md5sum', 'experimental_strategy',
    'object_id','platform'
    ]

GEN3_ALIQUOT_COLUMNS = ['type' ,'project_id','submitter_id','samples.submitter_id','aliquot_quantity',
                'aliquot_volume','amount','analyte_type','analyte_type_id','concentration',
                'source_center']

def find_idats(row):
    basename = row['Basename']
    red_idat = Path(f'{basename}_Red.idat')
    grn_idat = Path(f'{basename}_Grn.idat')
    if not red_idat.exists(): red_idat = Path(f'{basename}_Red.IDAT')
    if not grn_idat.exists(): grn_idat = Path(f'{basename}_Grn.IDAT')

    if not red_idat.exists(): red_idat = None
    if not grn_idat.exists(): grn_idat = None
    return [grn_idat, red_idat]

def annotate_idats(row):
    global MD5SUMS_DF
    
    idat = Path(row['file_name'])
    if not idat:
        return row

    # name_hash = hashlib.md5(str(idat).encode()).hexdigest()
    # md5_cache_filename = Path(f'tmp/{name_hash}.md5')
    #log.debug(f'{MD5SUMS_DF=}')
    md5_previous = MD5SUMS_DF[MD5SUMS_DF['file_name'] == str(idat)]
    #log.debug(f'{idat=}')
    #log.debug(f'{md5_previous=}')
    if md5_previous.empty:
        raise RuntimeError()
        log.debug(f'md5summing {idat=}')
        md5sum = hashlib.md5(open(idat,'rb').read()).hexdigest()
        MD5SUMS_DF = MD5SUMS_DF.append({'file_name':str(idat), 'md5sum':md5sum}, ignore_index=True)
        MD5SUMS_DF.to_csv('tmp/__x__md5sums.csv', index=False)
        md5_previous = MD5SUMS_DF[MD5SUMS_DF['file_name'] == str(idat)]
    md5sum = md5_previous.iloc[0]['md5sum']
    row['submitter_id'] = idat.name
    row['file_size'] = idat.stat().st_size
    row['md5sum'] = md5sum
    return row


if __name__ == '__main__':

    log = logging.getLogger(__name__)
    logging.basicConfig(level=logging.DEBUG)
    
    MD5SUMS_DF = pd.read_csv(snakemake.input.md5sums)
    log.debug(f'{MD5SUMS_DF=}')

    # Which delimiter are we using on the Gen3 side?
    if snakemake.output.df.endswith('.tsv'): delimiter = '\t'
    else: delimiter = ','

    datasets = pd.read_csv(snakemake.input.datasets)
    datasets = datasets[datasets['category'] == 'gwas']
    log.debug(f'{datasets=}')

    datasets = datasets.rename(columns={'subject': 'submitter_id'})

    methylation_filesets = []
    for (d, dataset) in datasets.iterrows():
        base = Path(dataset['url'])
        sheet_path = base/'.chammps/sample-manifest.csv'
        if sheet_path.exists():
            manifest = pd.read_csv(sheet_path)
            manifest['assay_instrument_model'] = 'Illumina Infinium HumanMethylation450K'
            manifest['file_name'] = manifest.apply(find_idats, axis=1)
            manifest = manifest.explode('file_name')
            manifest = manifest.apply(annotate_idats, axis=1)
            methylation_filesets.append(manifest)

    submitted_methylation = pd.concat(methylation_filesets)

    ## required columns
    genotyping_array = genotyping_array.rename(columns={'S_SAMPLEID':'aliquots.submitter_id'})
    genotyping_array['type'] = GEN3_TYPE

    genotyping_array['assay_instrument'] = 'Illumina'
    genotyping_array['assay_method'] = 'Methylation Array'
    genotyping_array['data_category'] = 'Methylation Data'
    genotyping_array['data_format'] = 'IDAT'
    genotyping_array['data_type'] = 'Methylation Intensity Values'
    genotyping_array['project_id'] = PROJECT_ID
    genotyping_array['projects.code'] = PROJECT_CODE
    genotyping_array['guid'] = datasets.apply(lambda x: uuid.uuid4(), axis=1)

    MD5SUMS_DF.to_csv(snakemake.input.md5sums, index=False)

    for colname in GEN3_COLUMNS:
        if colname not in genotyping_array.columns:
            genotyping_array[colname] = None
    
    genotyping_array.to_csv(snakemake.output.df, index=False, sep=delimiter, columns=GEN3_COLUMNS)

    #gen3_samples = pd.read_csv(snakemake.input.samples)
    #log.debug(f'{gen3_samples=}')    
    #methylation_samples = submitted_methylation[['aliquots.submitter_id']]
    #methylation_samples = methylation_samples.merge(gen3_samples, how='left', left_on='aliquots.submitter_id', right_on='submitter_id')
    #methylation_samples = samples.rename(columns={
    #    'aliquots.submitter_id': 'submitter_id',
    #    # 'S_SUBJECTID': 'subjects.submitter_id'
    #}
    #)
    #methylation_samples['project_id'] = 'g0-p0'
    #methylation_samples['type'] = 'sample'
    #methylation_samples['sample_type'] = 'Unknown'
    #methylation_samples.to_csv(snakemake.output.samples, index=False, sep=delimiter, columns=GEN3_ALIQUOT_COLUMNS)
    
    gen3_aliquots = pd.read_csv(snakemake.input.aliquots, sep=delimiter)
    log.debug(f'{gen3_aliquots=}')
    genotyping_aliquots = genotyping_array[['aliquots.submitter_id']]
    log.debug(f'{genotyping_aliquots=}')
    genotyping_aliquots = genotyping_aliquots.merge(gen3_aliquots, how='left', left_on='aliquots.submitter_id', right_on='submitter_id')
    genotyping_aliquots = genotyping_aliquots.drop_duplicates('submitter_id')
    log.debug(f'{genotyping_aliquots=}')
    genotyping_aliquots.to_csv(snakemake.output.aliquots, index=False, sep=delimiter, columns=GEN3_ALIQUOT_COLUMNS)

