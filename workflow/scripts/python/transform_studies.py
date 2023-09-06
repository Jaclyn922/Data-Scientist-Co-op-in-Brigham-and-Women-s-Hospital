import logging
import pandas as pd
import uuid

GEN3_TYPE = 'study'
PROJECT_CODE = 'p0'
PROJECT_ID = 'g0-p0'
RENAME_COLUMNS = {
    's_studyid':     'submitter_id',
    'studydesc':     'study_description',
    'u_studydesign': 'study_design'
}

if __name__ == '__main__':

    study = pd.read_csv(snakemake.input.studies)
    study = study.rename(columns=RENAME_COLUMNS)

    # Define the column names to be added
    # rejpz: only add these if we are going to use them; if blank, just ignore
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
    study['project_id'] = PROJECT_ID
    study['guid'] = study.apply(lambda x: uuid.uuid4(), axis=1)
    study['study_description'] = study['study_description'].fillna(study['submitter_id'])

    study = study[['submitter_id', 'study_description', 'study_design', 'type', 'projects.code', 'project_id','guid']]


    # Export the DataFrame as a TSV file
    if snakemake.output.studies.endswith('.tsv'):
        delimiter = '\t'
        study = study.drop(columns=['guid',])
    else:
        delimiter = ','

    study.to_csv(snakemake.output.studies, index=False, sep=delimiter)
