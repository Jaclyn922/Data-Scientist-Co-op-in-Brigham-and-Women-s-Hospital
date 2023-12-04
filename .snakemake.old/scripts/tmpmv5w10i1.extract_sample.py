
######## snakemake preamble start (automatically inserted, do not edit) ########
import sys; sys.path.extend(['/proj/relibs/relib00/conda-cdnm/envs/sm7/lib/python3.10/site-packages', '/udd/rexin/.cache/snakemake/snakemake/source-cache/runtime-cache/tmpaa8sioup/file/udd/rexin/code/etl/workflow/scripts/python', '/udd/rexin/code/etl/workflow/scripts/python']); import pickle; snakemake = pickle.loads(b'\x80\x04\x95\xfd\x03\x00\x00\x00\x00\x00\x00\x8c\x10snakemake.script\x94\x8c\tSnakemake\x94\x93\x94)\x81\x94}\x94(\x8c\x05input\x94\x8c\x0csnakemake.io\x94\x8c\nInputFiles\x94\x93\x94)\x81\x94}\x94(\x8c\x06_names\x94}\x94\x8c\x12_allowed_overrides\x94]\x94(\x8c\x05index\x94\x8c\x04sort\x94eh\x0f\x8c\tfunctools\x94\x8c\x07partial\x94\x93\x94h\x06\x8c\x19Namedlist._used_attribute\x94\x93\x94\x85\x94R\x94(h\x15)}\x94\x8c\x05_name\x94h\x0fsNt\x94bh\x10h\x13h\x15\x85\x94R\x94(h\x15)}\x94h\x19h\x10sNt\x94bub\x8c\x06output\x94h\x06\x8c\x0bOutputFiles\x94\x93\x94)\x81\x94\x8c\x0ftmp/samples.csv\x94a}\x94(h\x0b}\x94\x8c\x02df\x94K\x00N\x86\x94sh\r]\x94(h\x0fh\x10eh\x0fh\x13h\x15\x85\x94R\x94(h\x15)}\x94h\x19h\x0fsNt\x94bh\x10h\x13h\x15\x85\x94R\x94(h\x15)}\x94h\x19h\x10sNt\x94bh&h#ub\x8c\x06params\x94h\x06\x8c\x06Params\x94\x93\x94)\x81\x94}\x94(h\x0b}\x94h\r]\x94(h\x0fh\x10eh\x0fh\x13h\x15\x85\x94R\x94(h\x15)}\x94h\x19h\x0fsNt\x94bh\x10h\x13h\x15\x85\x94R\x94(h\x15)}\x94h\x19h\x10sNt\x94bub\x8c\twildcards\x94h\x06\x8c\tWildcards\x94\x93\x94)\x81\x94}\x94(h\x0b}\x94h\r]\x94(h\x0fh\x10eh\x0fh\x13h\x15\x85\x94R\x94(h\x15)}\x94h\x19h\x0fsNt\x94bh\x10h\x13h\x15\x85\x94R\x94(h\x15)}\x94h\x19h\x10sNt\x94bub\x8c\x07threads\x94K\x01\x8c\tresources\x94h\x06\x8c\tResources\x94\x93\x94)\x81\x94(K\x01K\x01M\xe8\x03M\xe8\x03\x8c\x19/d/q4/9885036.1.linux01.q\x94e}\x94(h\x0b}\x94(\x8c\x06_cores\x94K\x00N\x86\x94\x8c\x06_nodes\x94K\x01N\x86\x94\x8c\x06mem_mb\x94K\x02N\x86\x94\x8c\x07disk_mb\x94K\x03N\x86\x94\x8c\x06tmpdir\x94K\x04N\x86\x94uh\r]\x94(h\x0fh\x10eh\x0fh\x13h\x15\x85\x94R\x94(h\x15)}\x94h\x19h\x0fsNt\x94bh\x10h\x13h\x15\x85\x94R\x94(h\x15)}\x94h\x19h\x10sNt\x94bhWK\x01hYK\x01h[M\xe8\x03h]M\xe8\x03h_hTub\x8c\x03log\x94h\x06\x8c\x03Log\x94\x93\x94)\x81\x94}\x94(h\x0b}\x94h\r]\x94(h\x0fh\x10eh\x0fh\x13h\x15\x85\x94R\x94(h\x15)}\x94h\x19h\x0fsNt\x94bh\x10h\x13h\x15\x85\x94R\x94(h\x15)}\x94h\x19h\x10sNt\x94bub\x8c\x06config\x94}\x94\x8c\x04rule\x94\x8c\x0eextract_sample\x94\x8c\x0fbench_iteration\x94N\x8c\tscriptdir\x94\x8c+/udd/rexin/code/etl/workflow/scripts/python\x94ub.'); from snakemake.logging import logger; logger.printshellcmds = True; __real_file__ = __file__; __file__ = '/udd/rexin/code/etl/workflow/scripts/python/extract_sample.py';
######## snakemake preamble end #########
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