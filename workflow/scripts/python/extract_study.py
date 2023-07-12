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
    q_study = session.query(Study).filter()
    study = pd.read_sql(q_study.statement, session.bind)
    study = study.drop(columns=['estimatedsamples', 'usersequence', 'notes','collectionstartdt','collectionenddt','auditsequence',
                            'primaryaffection', 'tracelogid', 'createdt', 'createby','createtool', 'moddt', 'completeddt','cancelledby','cancelleddt',
                            'u_verifiedby','u_numberofsites','u_legacyid','securityuser','securitydepartment',
                            'modby','modtool','projectid','proposedstartdt','proposedenddt','startdt','clinicalflag','plannedsites','plannedparticipants','completedby',
                            'enddt','studyfocus','templateflag','subjectrequiredflag','priority','collectinforequiredflag','protocolname','hipaaflag',
                            'departmentid','verifiedby','conservativecocflag','conservativerestrictionsflag',
                            'verifiedbyrole'])
    study.to_csv(snakemake.output.df)