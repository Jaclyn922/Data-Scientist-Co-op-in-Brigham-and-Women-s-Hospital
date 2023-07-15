import logging
import pandas as pd

if __name__ == '__main__':

    subjects = pd.read_csv(snakemake.input.subjects)
    subjects = subjects.rename(columns={'S_SUBJECTID': 'submitter_id'})

    new_columns = ['consent_codes', 'disease_type', 'primary_site']

    # Define the column names to be added
    # Add the new columns with 'N/A' as the default value
    for column in new_columns:
        subjects[column] =  None

    # Add the new column named type
    subjects['type'] = 'case'

    # Export the DataFrame as a TSV file
    subjects.to_csv(snakemake.output.cases, sep='\t', index=False)
