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
    # select columns
    samplestable = samples[['projectid','submitterid','sampletypeid','S_SAMPLEID','initialmass']]
    samplestable.rename(columns={'initialmass':'initial_weight'})
                
    # add new columns
    new_columns =['cases.submitter_id','diagnoses.submitter_id','biospecimen_anatomic_site','composition','current_weight','days_to_collection','dats_to_sample_procurement','diagnosis_pathologically_confirmed','freezing_method','intermediate_dimension','is_ffpe','longest_dimension','method_of_sample_procurement','oct_embedded','preservation_method','sample_volume','shortest_dimension','time_between_clamping_and_freezing','time_between_excision_and_freezing','tissue_type','tumor_code','tumor_code_id','tumor_descriptor']

    # Add the new columns with 'N/A' as the default value
    for column in new_columns:
        samplestable[column] =  None

    #add type 
    samplestable['type'] ='sample'
   
    samplestable.to_csv(snakemake.output.df)