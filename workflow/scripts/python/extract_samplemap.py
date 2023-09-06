import pandas as pd

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker

DBURL = 'oracle://sapphire_r:sapphire_r0@oar1.bwh.harvard.edu:1521/labvprd'

sapphire8_md = MetaData(schema='SAPPHIRE8')
engine = create_engine(DBURL)
Session = sessionmaker(bind=engine)
session = Session()

Sample = Table('S_SAMPLE', sapphire8_md, autoload=True, autoload_with=engine)
SampleMap = Table('S_SAMPLEMAP', sapphire8_md, autoload=True, autoload_with=engine)
SampleFamily = Table('S_SAMPLEFAMILY', sapphire8_md, autoload=True, autoload_with=engine)

if __name__ == '__main__':
    query = session.query(SampleMap).filter()
    df = pd.read_sql(query.statement, session.bind, parse_dates={},)
    df.to_csv(snakemake.output.df)