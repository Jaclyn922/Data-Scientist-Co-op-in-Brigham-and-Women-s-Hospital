import pandas as pd

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker

DBURL = 'oracle://sapphire_r:sapphire_r0@oar1.bwh.harvard.edu:1521/labvprd'

sapphire8_md = MetaData(schema='SAPPHIRE8')
engine = create_engine(DBURL)
Session = sessionmaker(bind=engine)
session = Session()

Collection = Table('U_COLLECTION',sapphire8_md,autoload=True,autoload_with=engine)

if __name__ == '__main__':
    q_collections = session.query(Collection)
    collections = pd.read_sql(q_collections.statement, session.bind)
    collections.drop(columns=['usersequence', 'auditsequence', 'tracelogid', 'timepointname', 'timepointstart', 'timepointend',
                                'timepointtimeunits', 'visitstart',  'visitname', 'visittimeunits', 'visitend', 'alternatename',
                                'receivableflag', 'restrictionclassid', 'protocolname', 'startdt', 'enddt', 'templateflag',
                                'notes', 'createtool', 'modtool'], inplace=True)
    collections.to_csv(snakemake.output.df)
