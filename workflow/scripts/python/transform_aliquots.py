import logging
import pandas as pd

if __name__ == '__main__':

    samples = pd.read_csv(snakemake.input.samples)

    samples = samples[['projectid','submitterid','sampletypeid','S_SAMPLEID','initialmass']]
    samples = samples.rename(columns={'S_SAMPLEID': 'submitter_id'})
    samples.rename(columns={'initialmass':'initial_weight'})

    aliquots = samples[['submitterid']]
    aliquots = aliquots.rename(columns={'submitterid': 'sample_id'})
    
    # Define the column names to be added
    new_columns = ['samples.submitter_id','aliquot_quantity','aliquot_volume','amount','analyte_type',
                   'analyte_type_id','concentration','source_center']

    # Add the new columns with 'N/A' as the default value
    for column in new_columns:
        samples[column] =  None

    # Add the new column named type
    samples['type'] = 'aliquot'

    # Export the DataFrame as a TSV file
    samples.to_csv(snakemake.output.aliquots, sep='\t', index=False)
