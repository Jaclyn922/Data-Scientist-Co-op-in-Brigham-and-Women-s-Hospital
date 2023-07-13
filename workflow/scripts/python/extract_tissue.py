import pandas as pd

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker

DBURL = 'oracle://sapphire_r:sapphire_r0@oar1.bwh.harvard.edu:1521/labvprd'

sapphire8_md = MetaData(schema='SAPPHIRE8')
engine = create_engine(DBURL)
Session = sessionmaker(bind=engine)
session = Session()

Tissue = Table('S_TISSUE',sapphire8_md,autoload=True,autoload_with=engine)

if __name__ == '__main__':
    q_tissues = session.query(Tissue)
    tissues = pd.read_sql(q_tissues.statement, session.bind)
    tissues.to_csv(snakemake.output.df)
