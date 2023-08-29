import hashlib
import logging
import pandas as pd
from pathlib import Path
import uuid

GEN3_TYPE = 'submitted_methylation'
PROJECT_CODE = 'p0'

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
    idat = Path(row['file_name'])
    if not idat:
        return row

    name_hash = hashlib.md5(str(idat).encode()).digest()
    md5_cache_filename = Path(f'tmp/{name_hash}.md5')
    if md5_cache_filename.exists():
        md5sum = open(md5_cache_filename).read()
    else:
        md5sum = hashlib.md5(open(idat,'rb').read()).hexdigest()
        with open(md5_cache_filename, 'wb') as fh:
            fh.write(md5sum)
    row['submitter_id'] = idat.name
    row['file_size'] = idat.stat().st_size
    row['md5sum'] = md5sum
    return row


if __name__ == '__main__':

    log = logging.getLogger(__name__)
    logging.basicConfig(level=logging.DEBUG)
    
    datasets = pd.read_csv(snakemake.input.datasets)
    datasets = datasets[datasets['category'] == 'epigenetic/methylation']
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
    submitted_methylation = submitted_methylation.rename(columns={'S_SAMPLEID':'aliquots.submitter_id'})
    submitted_methylation['type'] = GEN3_TYPE
 
    submitted_methylation['assay_instrument'] = 'Illumina'
    submitted_methylation['assay_method'] = 'Methylation Array'
    submitted_methylation['data_category'] = 'Methylation Data'
    submitted_methylation['data_format'] = 'IDAT'
    submitted_methylation['data_type'] = 'Methylation Intensity Values'
    submitted_methylation['projects.code'] = PROJECT_CODE
    submitted_methylation['guid'] = datasets.apply(lambda x: uuid.uuid4(), axis=1)

    # Export the DataFrame as a TSV file
    submitted_methylation.to_csv(snakemake.output.df, index=False)
