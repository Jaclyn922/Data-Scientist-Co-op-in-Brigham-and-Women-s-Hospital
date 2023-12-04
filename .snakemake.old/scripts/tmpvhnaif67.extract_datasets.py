
######## snakemake preamble start (automatically inserted, do not edit) ########
import sys; sys.path.extend(['/proj/relibs/relib00/conda-cdnm/envs/sm7/lib/python3.10/site-packages', '/udd/rexin/.cache/snakemake/snakemake/source-cache/runtime-cache/tmpju2fgoze/file/udd/rexin/code/etl/workflow/scripts/python', '/udd/rexin/code/etl/workflow/scripts/python']); import pickle; snakemake = pickle.loads(b'\x80\x04\x95\x00\x04\x00\x00\x00\x00\x00\x00\x8c\x10snakemake.script\x94\x8c\tSnakemake\x94\x93\x94)\x81\x94}\x94(\x8c\x05input\x94\x8c\x0csnakemake.io\x94\x8c\nInputFiles\x94\x93\x94)\x81\x94}\x94(\x8c\x06_names\x94}\x94\x8c\x12_allowed_overrides\x94]\x94(\x8c\x05index\x94\x8c\x04sort\x94eh\x0f\x8c\tfunctools\x94\x8c\x07partial\x94\x93\x94h\x06\x8c\x19Namedlist._used_attribute\x94\x93\x94\x85\x94R\x94(h\x15)}\x94\x8c\x05_name\x94h\x0fsNt\x94bh\x10h\x13h\x15\x85\x94R\x94(h\x15)}\x94h\x19h\x10sNt\x94bub\x8c\x06output\x94h\x06\x8c\x0bOutputFiles\x94\x93\x94)\x81\x94\x8c\x10tmp/datasets.csv\x94a}\x94(h\x0b}\x94\x8c\x02df\x94K\x00N\x86\x94sh\r]\x94(h\x0fh\x10eh\x0fh\x13h\x15\x85\x94R\x94(h\x15)}\x94h\x19h\x0fsNt\x94bh\x10h\x13h\x15\x85\x94R\x94(h\x15)}\x94h\x19h\x10sNt\x94bh&h#ub\x8c\x06params\x94h\x06\x8c\x06Params\x94\x93\x94)\x81\x94}\x94(h\x0b}\x94h\r]\x94(h\x0fh\x10eh\x0fh\x13h\x15\x85\x94R\x94(h\x15)}\x94h\x19h\x0fsNt\x94bh\x10h\x13h\x15\x85\x94R\x94(h\x15)}\x94h\x19h\x10sNt\x94bub\x8c\twildcards\x94h\x06\x8c\tWildcards\x94\x93\x94)\x81\x94}\x94(h\x0b}\x94h\r]\x94(h\x0fh\x10eh\x0fh\x13h\x15\x85\x94R\x94(h\x15)}\x94h\x19h\x0fsNt\x94bh\x10h\x13h\x15\x85\x94R\x94(h\x15)}\x94h\x19h\x10sNt\x94bub\x8c\x07threads\x94K\x01\x8c\tresources\x94h\x06\x8c\tResources\x94\x93\x94)\x81\x94(K\x01K\x01M\xe8\x03M\xe8\x03\x8c\x19/d/q4/9707828.1.linux01.q\x94e}\x94(h\x0b}\x94(\x8c\x06_cores\x94K\x00N\x86\x94\x8c\x06_nodes\x94K\x01N\x86\x94\x8c\x06mem_mb\x94K\x02N\x86\x94\x8c\x07disk_mb\x94K\x03N\x86\x94\x8c\x06tmpdir\x94K\x04N\x86\x94uh\r]\x94(h\x0fh\x10eh\x0fh\x13h\x15\x85\x94R\x94(h\x15)}\x94h\x19h\x0fsNt\x94bh\x10h\x13h\x15\x85\x94R\x94(h\x15)}\x94h\x19h\x10sNt\x94bhWK\x01hYK\x01h[M\xe8\x03h]M\xe8\x03h_hTub\x8c\x03log\x94h\x06\x8c\x03Log\x94\x93\x94)\x81\x94}\x94(h\x0b}\x94h\r]\x94(h\x0fh\x10eh\x0fh\x13h\x15\x85\x94R\x94(h\x15)}\x94h\x19h\x0fsNt\x94bh\x10h\x13h\x15\x85\x94R\x94(h\x15)}\x94h\x19h\x10sNt\x94bub\x8c\x06config\x94}\x94\x8c\x04rule\x94\x8c\x10extract_datasets\x94\x8c\x0fbench_iteration\x94N\x8c\tscriptdir\x94\x8c+/udd/rexin/code/etl/workflow/scripts/python\x94ub.'); from snakemake.logging import logger; logger.printshellcmds = False; __real_file__ = __file__; __file__ = '/udd/rexin/code/etl/workflow/scripts/python/extract_datasets.py';
######## snakemake preamble end #########
import pandas as pd
from redminelib import Redmine
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
    REDMINE_STUDY = 'ESCAPE'
    redmine = redminelib.Redmine('https://chanmine.bwh.harvard.edu/', key='bc92021bd829b2c07aa94b3bc2679c639204b902', requests={'verify': False})
    trackers = pd.DataFrame(data=[dict(d) for d in redmine.tracker.all()])

    projects = redmine.project.all()
    projects = pd.DataFrame([dict(d) for d in projects])
    projects = projects.drop(columns=['wiki_pages', 'memberships', 'issue_categories', 'time_entries', 'versions', 'news',
                                      'issues', 'files', 'trackers', 'enabled_modules', 'time_entry_activities', 'issue_custom_fields',
                                      'is_public', 'inherit_members', 'status', 'created_on', 'updated_on'])

    study_project_id = projects.loc[projects['name'] == REDMINE_STUDY, 'id'].iloc[0]
    def set_url(row):
        cfs = row['custom_fields']
        for cf in cfs:
            cf_name = cf['name']
            if cf_name == 'URL':
                return cf['value']
        return None

    dataset_tracker_id = trackers.loc[trackers['name'] == 'Dataset', 'id'].iloc[0]
    datasets = redmine.issue.filter(tracker_id=dataset_tracker_id)
    datasets = pd.DataFrame([dict(d) for d in datasets])

    datasets['project_name'] = datasets.apply(lambda w: w.project['name'], axis=1)
    datasets['category_name'] = datasets.apply(lambda w: w.category['name'] if pd.notna(w.category) else None, axis=1)
    datasets['status_name'] = datasets.apply(lambda w: w.status['name'] if pd.notna(w.status) else None, axis=1)
    datasets['author_name'] = datasets.apply(lambda w: w.author['name'] if pd.notna(w.author) else None, axis=1)
    datasets['assignee'] = datasets.apply(lambda w: w.assigned_to['name'] if pd.notna(w.assigned_to) else None, axis=1)
    datasets['parent_id'] = datasets.apply(lambda w: int(w.parent['id']) if pd.notna(w.parent) else None, axis=1)
    datasets['url'] = datasets.apply(set_url, axis=1)

    datasets = datasets.sort_values(['project_name', 'category_name', 'subject'])
    datasets = datasets.rename(columns={'project_name': 'project', 'category_name': 'category', 'status_name': 'status', 'parent_id': 'parent'})

    datasets['omic'] = '?'

    datasets['subject'] = datasets.apply(lambda row: f"(#{row['id']}) {row['subject']}", axis=1)

    datasets = datasets.drop(columns=['allowed_statuses', 'assigned_to', 'assignee', 'attachments', 'author', 'author_name', 'category', 'category', 'changesets', 'children', 'closed_on', 'created_on', 'custom_fields', 'description', 'done_ratio', 'due_date', 'estimated_hours', 'is_private', 'journals', 'omic', 'parent', 'parent', 'priority', 'relations', 'start_date', 'status', 'status', 'time_entries', 'tracker', 'updated_on', 'watchers'])

    datasets.to_csv(snakemake.output.df)