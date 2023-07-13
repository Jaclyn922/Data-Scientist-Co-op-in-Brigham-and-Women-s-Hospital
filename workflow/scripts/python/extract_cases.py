import pandas as pd

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker

DBURL = 'oracle://sapphire_r:sapphire_r0@oar1.bwh.harvard.edu:1521/labvprd'

sapphire8_md = MetaData(schema='SAPPHIRE8')
engine = create_engine(DBURL)
Session = sessionmaker(bind=engine)
session = Session()

Sample = Table('S_SAMPLE',sapphire8_md,autoload=True,autoload_with=engine)
SampleFamily = Table('S_SAMPLEFAMILY',sapphire8_md,autoload=True,autoload_with=engine)

if __name__ == '__main__':
    # Group by 'S_SUBJECTID' and 'experiments.submitter_id'
    cases = samples.groupby(['S_SUBJECTID', 'experiments.submitter_id']).agg(list)

    # Define the new columns to add
    new_columns = ['consent_codes', 'disease_type', 'primary_site']

    # Define a function to add new columns with default values to each group
    def add_default_columns(group):
        for column in new_columns:
            group[column] = 'N/A'
        return group

    # Apply the function to each group and reassemble the DataFrame
    cases = cases.apply(add_default_columns, group_keys=False).reset_index(drop=True)

    # Add the new column named type    
    datasets['type'] = 'case'
    cases.to_csv(snakemake.output.df)