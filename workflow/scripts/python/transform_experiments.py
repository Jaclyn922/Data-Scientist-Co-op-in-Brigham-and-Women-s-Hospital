import logging
import pandas as pd
import uuid

GEN3_TYPE = 'dataset'
PROJECT_CODE = 'p0'

if __name__ == '__main__':

    datasets = pd.read_csv(snakemake.input.datasets)
    datasets = datasets.rename(columns={'subject': 'submitter_id'})

    # Define the column names to be added
    new_columns = ['associated_experiment', 'copy_numbers_identified', 'data_description', 'experimental_description',
                   'experimental_intent', 'indels_identified', 'marker_panel_description', 'number_experimental_group',
                   'number_samples_per_experimental_group', 'number_samples_per_experimental_group',
                   'somatic_mutations_identified', 'type_of_data', 'type_of_sample', 'type_of_specimen']

    # Add the new columns with 'N/A' as the default value
    for column in new_columns:
        datasets[column] =  None

    # Add the new column named type
    datasets['type'] = GEN3_TYPE
    datasets['projects.code'] = PROJECT_CODE
    datasets['guid'] = datasets.apply(lambda x: uuid.uuid4(), axis=1)

    # Export the DataFrame as a TSV file
    datasets.to_csv(snakemake.output.datasets, index=False)
    datasets.to_csv(snakemake.output.freezes, index=False)
