import logging
import pandas as pd
import uuid

GEN3_TYPE = 'study'
PROJECT_CODE = 'p0'
RENAME_COLUMNS = {'s_studyid': 'submitter_id', 'studydesc':'study_description',}

if __name__ == '__main__':

    study = pd.read_csv(snakemake.input.studies)
    study = study.rename(columns=RENAME_COLUMNS)

    # Define the column names to be added
    #new_columns = ['associated_experiment', 'copy_numbers_identified', 'data_description', 'experimental_description',
    #               'experimental_intent', 'indels_identified', 'marker_panel_description', 'number_experimental_group',
    #               'number_samples_per_experimental_group', 'number_samples_per_experimental_group',
    #               'somatic_mutations_identified', 'type_of_data', 'type_of_sample', 'type_of_specimen']
    new_columns = []

    # Add the new columns with 'N/A' as the default value
    for column in new_columns:
        if column not in study:
            study[column] =  None

    # Add the new column named type
    study['type'] = GEN3_TYPE
    study['projects.code'] = PROJECT_CODE
    study['guid'] = study.apply(lambda x: uuid.uuid4(), axis=1)

    # Export the DataFrame as a TSV file
    study.to_csv(snakemake.output.studies, index=False)
