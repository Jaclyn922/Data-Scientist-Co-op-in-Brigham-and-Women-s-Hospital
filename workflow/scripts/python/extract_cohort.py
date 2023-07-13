import pandas as pd

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker

DBURL = 'oracle://sapphire_r:sapphire_r0@oar1.bwh.harvard.edu:1521/labvprd'

sapphire8_md = MetaData(schema='SAPPHIRE8')
engine = create_engine(DBURL)
Session = sessionmaker(bind=engine)
session = Session()

Subject = Table('S_SUBJECT',sapphire8_md,autoload=True,autoload_with=engine)
Cohort = Table('U_COHORT',sapphire8_md,autoload=True,autoload_with=engine)

if __name__ == '__main__':
    q_cohort = session.query(Cohort, Subject).filter(Subject.c.s_subjectid==Cohort.c.subjectid)
    cohort = pd.read_sql(q_cohort.statement, session.bind)
    cohort.drop(columns=['usersequence', 'auditsequence', 'tracelogid', 'templateflag', 'notes', 'createtool', 'modtool',
                    'moddt', 'cohortdesc', 'createdt', 'createby', 'modby', 'activeflag', 'subjecttype', 'subjectdesc',
                    'speciesid', 'strainid', 'securityuser', 'securitydepartment', 'activeflag_1', 'auditsequence_1',
                    'createby_1', 'createdt_1', 'createtool_1', 'modby_1', 'moddt_1', 'modtool_1', 'templateflag_1',
                     'tracelogid_1', 'usersequence_1', 'notes_1', 'u_cageid', 'u_country', 'affliction', 'severity',
                    ], inplace=True)
    
    cohort.to_csv(snakemake.output.df)
