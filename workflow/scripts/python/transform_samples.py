import logging
import pandas as pd

if __name__ == '__main__':

    samples = pd.read_csv(snakemake.input.samples)

    # select columns
    samples = samples[['projectid','submitterid','sampletypeid','S_SAMPLEID','initialmass']]
    samples = samples.rename(columns={'S_SAMPLEID': 'submitter_id'})
    samples.rename(columns={'initialmass':'initial_weight'})

    # Define the column names to be added
    new_columns = ['cases.submitter_id','diagnoses.submitter_id','biospecimen_anatomic_site','composition','current_weight',
    'days_to_collection','dats_to_sample_procurement','diagnosis_pathologically_confirmed','freezing_method','intermediate_dimension',
    'is_ffpe','longest_dimension','method_of_sample_procurement','oct_embedded','preservation_method','sample_volume',
    'shortest_dimension','time_between_clamping_and_freezing','time_between_excision_and_freezing','tissue_type',
    'tumor_code','tumor_code_id','tumor_descriptor']

    # Add the new columns with 'N/A' as the default value
    for column in new_columns:
        samples[column] =  None

    # Add the new column named type
    samples['type'] = 'sample'

    # Export the DataFrame as a TSV file
    samples.to_csv(snakemake.output.samples, sep='\t', index=False)
