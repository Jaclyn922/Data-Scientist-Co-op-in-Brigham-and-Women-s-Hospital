import pandas as pd
import redminelib

redmine = redminelib.Redmine('https://chanmine.bwh.harvard.edu/', key='bc92021bd829b2c07aa94b3bc2679c639204b902', requests={'verify': False})
    
if __name__ == '__main__':

    redmine = redminelib.Redmine('https://chanmine.bwh.harvard.edu/', key='bc92021bd829b2c07aa94b3bc2679c639204b902', requests={'verify': False})
    trackers = pd.DataFrame(data=[dict(d) for d in redmine.tracker.all()])

    projects = redmine.project.all()
    projects = pd.DataFrame([dict(d) for d in projects])
    projects = projects.drop(columns=['wiki_pages', 'memberships', 'issue_categories', 'time_entries', 'versions', 'news',
                                      'issues', 'files', 'trackers', 'enabled_modules', 'time_entry_activities', 'issue_custom_fields',
                                      'is_public', 'inherit_members', 'status', 'created_on', 'updated_on'])

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
