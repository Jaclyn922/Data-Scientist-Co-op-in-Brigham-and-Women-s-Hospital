
######## snakemake preamble start (automatically inserted, do not edit) ########
import sys; sys.path.extend(['/proj/relibs/relib00/conda-cdnm/envs/sm7/lib/python3.10/site-packages', '/udd/rexin/.cache/snakemake/snakemake/source-cache/runtime-cache/tmpcuenxim7/file/udd/rexin/code/etl/workflow/scripts/python', '/udd/rexin/code/etl/workflow/scripts/python']); import pickle; snakemake = pickle.loads(b'\x80\x04\x95\xa6\x05\x00\x00\x00\x00\x00\x00\x8c\x10snakemake.script\x94\x8c\tSnakemake\x94\x93\x94)\x81\x94}\x94(\x8c\x05input\x94\x8c\x0csnakemake.io\x94\x8c\nInputFiles\x94\x93\x94)\x81\x94(\x8c\x10tmp/datasets.csv\x94\x8c\x18tmp/genotyping_array.md5\x94\x8c:/d/docker/volumes/regep00/gen3/submissions/new/aliquot.csv\x94\x8c9/d/docker/volumes/regep00/gen3/submissions/new/sample.tsv\x94e}\x94(\x8c\x06_names\x94}\x94(\x8c\x08datasets\x94K\x00N\x86\x94\x8c\x07md5sums\x94K\x01N\x86\x94\x8c\x08aliquots\x94K\x02N\x86\x94\x8c\x07samples\x94K\x03N\x86\x94u\x8c\x12_allowed_overrides\x94]\x94(\x8c\x05index\x94\x8c\x04sort\x94eh\x1b\x8c\tfunctools\x94\x8c\x07partial\x94\x93\x94h\x06\x8c\x19Namedlist._used_attribute\x94\x93\x94\x85\x94R\x94(h!)}\x94\x8c\x05_name\x94h\x1bsNt\x94bh\x1ch\x1fh!\x85\x94R\x94(h!)}\x94h%h\x1csNt\x94bh\x11h\nh\x13h\x0bh\x15h\x0ch\x17h\rub\x8c\x06output\x94h\x06\x8c\x0bOutputFiles\x94\x93\x94)\x81\x94(\x8cT/d/docker/volumes/regep00/gen3/submissions/new/genotyping_array/genotyping_array.tsv\x94\x8cK/d/docker/volumes/regep00/gen3/submissions/new/genotyping_array/aliquot.tsv\x94e}\x94(h\x0f}\x94(\x8c\x02df\x94K\x00N\x86\x94h\x15K\x01N\x86\x94uh\x19]\x94(h\x1bh\x1ceh\x1bh\x1fh!\x85\x94R\x94(h!)}\x94h%h\x1bsNt\x94bh\x1ch\x1fh!\x85\x94R\x94(h!)}\x94h%h\x1csNt\x94bh3h/h\x15h0ub\x8c\x06params\x94h\x06\x8c\x06Params\x94\x93\x94)\x81\x94}\x94(h\x0f}\x94h\x19]\x94(h\x1bh\x1ceh\x1bh\x1fh!\x85\x94R\x94(h!)}\x94h%h\x1bsNt\x94bh\x1ch\x1fh!\x85\x94R\x94(h!)}\x94h%h\x1csNt\x94bub\x8c\twildcards\x94h\x06\x8c\tWildcards\x94\x93\x94)\x81\x94\x8c\x03tsv\x94a}\x94(h\x0f}\x94\x8c\x06suffix\x94K\x00N\x86\x94sh\x19]\x94(h\x1bh\x1ceh\x1bh\x1fh!\x85\x94R\x94(h!)}\x94h%h\x1bsNt\x94bh\x1ch\x1fh!\x85\x94R\x94(h!)}\x94h%h\x1csNt\x94b\x8c\x06suffix\x94hRub\x8c\x07threads\x94K\x01\x8c\tresources\x94h\x06\x8c\tResources\x94\x93\x94)\x81\x94(K\x01K\x01M\xe8\x03M\xe8\x03\x8c\x04/tmp\x94e}\x94(h\x0f}\x94(\x8c\x06_cores\x94K\x00N\x86\x94\x8c\x06_nodes\x94K\x01N\x86\x94\x8c\x06mem_mb\x94K\x02N\x86\x94\x8c\x07disk_mb\x94K\x03N\x86\x94\x8c\x06tmpdir\x94K\x04N\x86\x94uh\x19]\x94(h\x1bh\x1ceh\x1bh\x1fh!\x85\x94R\x94(h!)}\x94h%h\x1bsNt\x94bh\x1ch\x1fh!\x85\x94R\x94(h!)}\x94h%h\x1csNt\x94bhiK\x01hkK\x01hmM\xe8\x03hoM\xe8\x03hqhfub\x8c\x03log\x94h\x06\x8c\x03Log\x94\x93\x94)\x81\x94}\x94(h\x0f}\x94h\x19]\x94(h\x1bh\x1ceh\x1bh\x1fh!\x85\x94R\x94(h!)}\x94h%h\x1bsNt\x94bh\x1ch\x1fh!\x85\x94R\x94(h!)}\x94h%h\x1csNt\x94bub\x8c\x06config\x94}\x94\x8c\x04rule\x94\x8c\x12T_genotyping_array\x94\x8c\x0fbench_iteration\x94N\x8c\tscriptdir\x94\x8c+/udd/rexin/code/etl/workflow/scripts/python\x94ub.'); from snakemake.logging import logger; logger.printshellcmds = True; __real_file__ = __file__; __file__ = '/udd/rexin/code/etl/workflow/scripts/python/transform_genotyping_array.py';
######## snakemake preamble end #########
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

    genotyping_filesets = []
    for (d, dataset) in datasets.iterrows():
        base = Path(dataset['url'])
        sheet_path = base/'.chammps/sample-manifest.csv'
        if sheet_path.exists():
            manifest = pd.read_csv(sheet_path)
        
            manifest['file_name'] = manifest.apply(find_idats, axis=1)
            manifest = manifest.explode('file_name')
            manifest = manifest.apply(annotate_idats, axis=1)
            genotyping_filesets.append(manifest)

    genotyping_array = pd.concat(genotyping_filesets)

    ## required columns
    genotyping_array = genotyping_array.rename(columns={'S_SAMPLEID':'aliquots.submitter_id'})
    genotyping_array['type'] = GEN3_TYPE
    genotyping_array['platform'] = 'Illumina OMNI 5M SNP Array'
    genotyping_array['experimental_strategy'] = 'Genotyping Array'
    genotyping_array['data_format'] = 'IDAT'
    genotyping_array['data_type'] = 'Genotyping Raw Intensity'
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
    
    sample_map = pd.read_csv(snakemake.input.sample_map)
    log.debug(f'{sample_map=}')
    def get_init_sample(sid):
        log.debug(f'{sid=}')
        parents = sample_map[sample_map['destsampleid']==sid]
        if len(parents) == 0:
            return sid
        elif len(parents) > 1:
            # raise RuntimeError('Pooled sample!')
            log.debug(f'pooled,{len(parents)=},{parents=}')
            p = []
            for pid in parents['sourcesampleid'].values:
                p.append(get_init_sample(pid))
            return ','.join(set(p))
        else:
            log.debug(f'found_one,{parents=},{parents["sourcesampleid"].values[0]}')
            return get_init_sample(parents['sourcesampleid'].values[0])
        
    def find_init_sample(row):
        """ """
        sid = row['submitter_id']
        return get_init_sample(sid)
    
    gen3_aliquots = pd.read_csv(snakemake.input.aliquots, sep=delimiter)
    log.debug(f'{gen3_aliquots=}')
    genotyping_aliquots = genotyping_array[['aliquots.submitter_id']]
    log.debug(f'{genotyping_aliquots=}')
    genotyping_aliquots = genotyping_aliquots.merge(gen3_aliquots, how='left', left_on='aliquots.submitter_id', right_on='submitter_id')
    genotyping_aliquots = genotyping_aliquots.drop_duplicates('submitter_id')
    log.debug(f'{genotyping_aliquots=}')
    genotyping_aliquots.to_csv(snakemake.output.aliquots, index=False, sep=delimiter, columns=GEN3_ALIQUOT_COLUMNS)

