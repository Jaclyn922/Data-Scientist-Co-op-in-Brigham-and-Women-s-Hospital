import logging
import pandas as pd
from pathlib import Path
import uuid


GEN3_TYPE = 'aligned_read'
PROJECT_CODE = 'p0'
PROJECT_ID = 'g0-p0'

# I used the property name on the table in google docs 
GEN3_COLUMNS = [
    'submitter_id', 'type', 'md5sum', 'aliquots.file_size', 'file_name',
    'platform', 'experimental_strategy', 'data_type', 'data_format', 'data_category']


GEN3_ALIQUOT_COLUMNS = ['sample_id','actual_id','batch','ALIASID','S_SAMPLEID','mapped_bam_path','batch_mapped','sample_name',
                        'mapped_bam_md5sum','sample','ALIASID_byalias','S_SAMPLEID_byalias','batch_byalias','sample_name_byalias',
                        'sample_id_unmapped','actual_id_unmapped','batch_unmapped','ALIASID_unmapped','S_SAMPLEID_unmapped',
                        'unmapped_bam_path','batch_unmapped','sample_name_unmapped','unmapped_bam_md5sum','ALIASID_byalias_unmapped',
                        'S_SAMPLEID_byalias_unmapped','batch_byalias_unmapped','sample_name_byalias_unmapped','n_mapped','n_unmapped']


# mapped_bam_path is the path of idat so don't need find_idats function
def annotate_bams(row):
    bam_file = Path(row['mapped_bam_path'])
    if not bam_file.exists():
        return row


    row['submitter_id'] = bam_file.name
    row['file_name'] = str(bam_file)
    row['file_size'] = bam_file.stat().st_size
    return row

if __name__ == '__main__':
    log = logging.getLogger(__name__)
    logging.basicConfig(level=logging.DEBUG)

    sample_table = pd.read_csv('/proj/regeps/regep00/studies/COPDGene/analyses/rejpz/freeze5_dbgap_submission/castaldi-freeze5-sample-table.pep.csv')
    log.debug(f'{sample_table=}')

    # Process the sample table directly using 'mapped_bam_path'
    sample_table = sample_table.dropna(subset=['mapped_bam_path'])
    sample_table = sample_table.apply(annotate_bams, axis=1)

    # Add or modify other columns as per your requirements
    sample_table['type'] = GEN3_TYPE
    sample_table['project_id'] = PROJECT_ID
    # Add other necessary columns and values

    # Export the DataFrame as a TSV file
    sample_table.to_csv(snakemake.output.aligned_read, sep='\t', index=False, columns=GEN3_COLUMNS)


# import hashlib
# import logging
# import pandas as pd
# from pathlib import Path
# import uuid
# from snakemake.shell import shell

# GEN3_TYPE = 'submitted_aligned_read'
# PROJECT_CODE = 'p0'
# PROJECT_ID = 'g0-p0'
# GEN3_COLUMNS = [
#     'type', 'project_id', 'submitter_id', 'aliquots.submitter_id', 'core_metadata_collections.submitter_id',
#     'data_category', 'data_format', 'data_type', 'file_name', 'file_size', 'md5sum', 'assay_instrument',
#     'assay_instrument_model', 'assay_method', 'object_id'
# ]

# GEN3_ALIQUOT_COLUMNS = ['type', 'project_id', 'submitter_id', 'samples.submitter_id', 'aliquot_quantity',
#                         'aliquot_volume', 'amount', 'analyte_type', 'analyte_type_id', 'concentration',
#                         'source_center']

# def annotate_idats(row):
#     global MD5SUMS_DF

#     idat = Path(row['mapped_bam_path'])
#     if not idat:
#         return row

#     md5_previous = MD5SUMS_DF[MD5SUMS_DF['file_name'] == str(idat)]

#     if md5_previous.empty:
#         log.debug(f'md5summing {idat=}')
#         md5sum = hashlib.md5(open(idat, 'rb').read()).hexdigest()
#         MD5SUMS_DF = MD5SUMS_DF.append({'file_name': str(idat), 'md5sum': md5sum}, ignore_index=True)
#         MD5SUMS_DF.to_csv('tmp/__x__md5sums.csv', index=False)
#         md5_previous = MD5SUMS_DF[MD5SUMS_DF['file_name'] == str(idat)]

#     md5sum = md5_previous.iloc[0]['md5sum']
#     row['submitter_id'] = idat.name
#     row['file_size'] = idat.stat().st_size
#     row['md5sum'] = md5sum
#     return row

# if __name__ == '__main__':
#     log = logging.getLogger(__name__)
#     logging.basicConfig(level=logging.DEBUG)

#     MD5SUMS_DF = pd.read_csv(snakemake.input.md5sums)
#     log.debug(f'{MD5SUMS_DF=}')

#     # Which delimiter are we using on the Gen3 side?
#     if snakemake.output.df.endswith('.tsv'):
#         delimiter = '\t'
#     else:
#         delimiter = ','

#     # Load table
#     sample_table = pd.read_csv("/proj/regeps/regep00/studies/COPDGene/analyses/rejpz/freeze5_dbgap_submission/castaldi-freeze5-sample-table.pep.csv")

#     sample_table = sample_table.rename(columns={'S_SAMPLEID': 'submitter_id'})

#     aligned_read_filesets = []

#     for _, sample in sample_table.iterrows():
#         mapped_bam_path = sample['mapped_bam_path']

#         if Path(mapped_bam_path).exists():
#             manifest = pd.DataFrame([sample])
#             manifest['assay_instrument_model'] = 'Illumina Infinium HumanMethylation450K'
#             manifest = manifest.apply(annotate_idats, axis=1)
#             aligned_read_filesets.append(manifest)

#     submitted_aligned_read = pd.concat(aligned_read_filesets)

#     submitted_aligned_read['type'] = GEN3_TYPE
#     submitted_aligned_read['assay_instrument'] = 'Assay_Instrument'
#     submitted_aligned_read['assay_method'] = 'Assay_Method'
#     submitted_aligned_read['data_category'] = 'Data_Category'
#     submitted_aligned_read['data_format'] = 'Data_Format'
#     submitted_aligned_read['data_type'] = 'Data_Type'
#     submitted_aligned_read['project_id'] = PROJECT_ID

#     submitted_aligned_read['guid'] = submitted_aligned_read.apply(lambda x: uuid.uuid4(), axis=1)

#     MD5SUMS_DF.to_csv(snakemake.input.md5sums, index=False)

#     for colname in GEN3_COLUMNS:
#         if colname not in submitted_aligned_read.columns:
#             submitted_aligned_read[colname] = None

#     submitted_aligned_read.to_csv(snakemake.output.df, index=False, sep=delimiter, columns=GEN3_COLUMNS)

