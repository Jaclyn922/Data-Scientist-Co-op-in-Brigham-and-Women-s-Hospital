import logging
import pandas as pd
import uuid

GEN3_COLUMNS = ['type' ,'project_id','submitter_id','samples.submitter_id','aliquot_quantity',
                'aliquot_volume','amount','analyte_type','analyte_type_id','concentration',
                'source_center']

if __name__ == '__main__':

    aliquots = pd.read_csv(snakemake.input.samples)
    # aliquots['ALIASID'].fillna(aliquots['S_SAMPLEID'])
    aliquots['submitter_id'] = aliquots['S_SAMPLEID']
    aliquots = aliquots.rename(columns={
        # 'ALIASID':      'submitter_id',
        'S_SAMPLEID':   'samples.submitter_id',
        'initialmass':  'initial_weight',
        'sampletypeid': 'analyte_type',
        }
    )
    aliquots = aliquots[aliquots['samples.submitter_id'].notna()]
    # Add the new column named type
    aliquots['project_id'] = 'g0-p0'
    aliquots['type'] = 'aliquot'
    aliquots['guid'] = aliquots.apply(lambda x: uuid.uuid4(), axis=1)

    # Add the new columns with 'N/A' as the default value
    for column in GEN3_COLUMNS:
        if column not in aliquots.columns:
            aliquots[column] =  None

    # Export the DataFrame as a TSV file
    aliquots = aliquots.drop_duplicates('submitter_id')
    aliquots.to_csv(snakemake.output.aliquots, sep='\t', index=False, columns=GEN3_COLUMNS)
