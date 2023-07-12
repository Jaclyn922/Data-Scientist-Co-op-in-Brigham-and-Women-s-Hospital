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
    STUDY = 'CAMP'
    q_sf = session.query(SampleFamily, Tissue.c.tissuedesc).filter(SampleFamily.c.sstudyid==STUDY,         Tissue.c.s_tissueid==SampleFamily.c.tissueid)
    sf = pd.read_sql(q_sf.statement, session.bind)
    sf.drop(columns=['samplefamilydesc', 'initialpackageid', 'initialdepartmentid', 'collectmethodid', 'verifiedby', 'verifieddt',
                'u_visitname', 'u_timepointname', 'approvedby', 'approveddt', 'recieveddt', 'cocflag', 'restrictionsflag', 'age',
                'ageunits', 'restrictclassid', 'externalsubject', 'animal', 'diseaseid', 'diseaseid', 'metastasisid', 'clinicaldiagid',
                'conditionalapprovalreason', 'conditionalapprovalflag', 'specimendefid', 'activeflag', 'auditsequence', 'clinicalevent',
 'clinicalprotocolid',
 'clinicalprotocolrevision',
 'clinicalprotocolversionid',
                 'createby',
 'createtool',
 'deviationdesc',
 'eventdefid',
 'initialmass',
 'initialmassunits',
 'initialvolume',
 'initialvolumeunits',
 'kittrackitem',
 'modby',
 'moddt',
 'modtool',
 'notes', 'tracelogid',  'usersequence', 'templateflag', 'participantid', 'u_legacyfamilyid', 'u_collectiontypid', 'sampletypeid',
                 'participanteventid', 'sstudyid',
                ], inplace=True)
    sf.to_csv(snakemake.output.df)