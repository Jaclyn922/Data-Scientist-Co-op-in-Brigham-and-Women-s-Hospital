import pandas as pd

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker

DBURL = 'oracle://sapphire_r:sapphire_r0@oar1.bwh.harvard.edu:1521/labvprd'

sapphire8_md = MetaData(schema='SAPPHIRE8')
engine = create_engine(DBURL)
Session = sessionmaker(bind=engine)
session = Session()

Package = Table('S_PACKAGE',sapphire8_md,autoload=True,autoload_with=engine)

if __name__ == '__main__':
    q_packages = session.query(Package)
    packages = pd.read_sql(q_packages.statement, session.bind)
    packages.drop(columns=['carrier', 'carriertype', 'trackingnumber', 'createby', 'createtool', 'moddt', 'modby', 'modtool', 'securityuser', 'securitydepartment',
                       'usersequence', 'notes', 'auditsequence', 'condition', 'senderaddressid', 'senderaddresstype', 'recipientaddressid', 'recipientaddresstype',
                       'tracelogid', 'templateflag', 'packagetype', 'senderdepartmentid', 'expecteddt', ], inplace=True)
    packages.to_csv(snakemake.output.df)
