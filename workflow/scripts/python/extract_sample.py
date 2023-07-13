import pandas as pd

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker

DBURL = 'oracle://sapphire_r:sapphire_r0@oar1.bwh.harvard.edu:1521/labvprd'

sapphire8_md = MetaData(schema='SAPPHIRE8')
engine = create_engine(DBURL)
Session = sessionmaker(bind=engine)
session = Session()

Sample = Table('S_SAMPLE', sapphire8_md, autoload=True, autoload_with=engine)
SampleFamily = Table('S_SAMPLEFAMILY', sapphire8_md, autoload=True, autoload_with=engine)

if __name__ == '__main__':
    q_samples = session.query(Sample.c.s_sampleid,
                            Sample.c.sampletypeid, Sample.c.createdt, Sample.c.samplefamilyid,
                            ).filter()
    #q_samples = session.query(Sample, SampleFamily).filter(SampleFamily.c.sstudyid=='CAMP', Sample.c.samplefamilyid==SampleFamily.c.s_samplefamilyid)
    samples = pd.read_sql(q_samples.statement, session.bind, parse_dates={},)
    #samples.drop(columns=['sampledesc', 'createby', 'createtool', 'submitterid', 'modby', 'submitteddt', 'modtool', 'notes', 'auditsequence', 'tracelogid',
    #                    'usersequence', 'activeflag', 'allocatedforaddressid', 'allocatedforaddresstype', 'allocatedfordepartmentid', 'auditsequence', 'autofinalrptflag',
    #                    'autoreceiveflag', 'batchstageid',  'cancelledby', 'cancelleddt', 'classification', 'cocflag', 'cocrequiredflag', 'collectedby', 'collectiondt',
    #                    'completedt', 'concentration', 'concentrationunits', 'conditionlabel', 'confirmedby', 'confirmeddt', 'controlsubstanceflag','deviations', 'disposaldt',
    #                    'disposalstatus', 'disposaltargetdt', 'disposedby', 'duedt', 'duedtoffset', 'duedtoffsettimeunit', 'duedtoverrideflag', 'eventdt', 'eventnum', 'eventplan',
    #                    'eventplanitem', 'glpflag', 'instrumentid', 'locationid', 'locationpath', 'moddt', 'monitorgroupid', 'numberlabels', 'physicalcondition', 'pooledflag',
    #                    'preptypeid', 'previousstoragestatus', 'priority', 'processinstruction', 'processtype', 'productid', 'productversionid', 'projectid', 'qcsampletype',
    #                    'reagentlotid', 'receivedby', 'receiveddt', 'receiverequiredflag', 'requestid', 'requestitemdetailid', 'requestitemid', 'restrictionsflag',
    #                    'reviewdisposition', 'reviewedby', 'revieweddt', 'reviewremarks', 'reviewrequiredflag', 'samplepointid', 'samplepointinstance', 'samplestatus',
    #                    'schedulerulelabel', 'scheduletemplateflag', 'sdiworkitemcompletionstatus', 'securitydepartment', 'securityuser', 'sourcesdiworkitemid',
    #                    'sourcespid', 'sourcesplevelid', 'sourcespsourcelabel', 'sourcespversionid', 'starttestingdt',
    #                    'storagedisposalstatus', 'studyid', 'samplesubtypeid', 'u_aliquotnumber', 'specimentype', 'treatments',
    #                    'storagestatus', 'templateflag',  'workorderid', 'basedonsampleid', 'u_qcfailureflag', 'u_qctype'], inplace=True)
    samples.to_csv(snakemake.output.df)