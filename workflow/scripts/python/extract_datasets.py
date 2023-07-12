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

# datasets = datasets.drop(columns=['time_entries', 'changesets', 'watchers', 'allowed_statuses', 'due_date', 'done_ratio', 'tracker', 'priority', 'start_date', 'updated_on',
#                                 'project', 'category', 'status', 'assigned_to', 'author', 'attachments', 'journals', 'is_private',
##                                 'estimated_hours', 'created_on', 'closed_on', 'parent', 'description', 'custom_fields', 'relations',
#                                 'children', 'author_name'])
datasets = datasets.sort_values(['project_name', 'category_name', 'subject'])
datasets = datasets.rename(columns={'project_name':'project','category_name':'category','status_name':'status','parent_id':'parent'})

# datasets = datasets[datasets['project'] == REDMINE_STUDY]

# datasets = datasets.drop(columns=['project', 'status'])

datasets['omic'] = '?'

### These will need to be fixed (made unique)
## datasets[datasets['subject'] == 'TopMed WGS Data']
datasets['subject'] = datasets.apply(lambda row: f"(#{row['id']}) {row['subject']}", axis=1)


datasets = datasets.drop(columns =['allowed_statuses',
 'assigned_to', 'assignee', 'attachments', 'author', 'author_name',  'category', 'category',
 'changesets', 'children', 'closed_on', 'created_on', 'custom_fields', 'description', 'done_ratio', 'due_date',
 'estimated_hours', 'is_private', 'journals', 'omic', 'parent', 'parent', 'priority', 'relations', 'start_date', 'status',
 'status', 'time_entries', 'tracker', 'updated_on', 'watchers'])