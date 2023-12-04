
######## snakemake preamble start (automatically inserted, do not edit) ########
import sys; sys.path.extend(['/proj/relibs/relib00/conda-cdnm/envs/sm7/lib/python3.10/site-packages', '/udd/rexin/.cache/snakemake/snakemake/source-cache/runtime-cache/tmp3mx79bu9/file/udd/rexin/code/etl/workflow/scripts/python', '/udd/rexin/code/etl/workflow/scripts/python']); import pickle; snakemake = pickle.loads(b'\x80\x04\x95b\x04\x00\x00\x00\x00\x00\x00\x8c\x10snakemake.script\x94\x8c\tSnakemake\x94\x93\x94)\x81\x94}\x94(\x8c\x05input\x94\x8c\x0csnakemake.io\x94\x8c\nInputFiles\x94\x93\x94)\x81\x94\x8c\rtmp/study.csv\x94a}\x94(\x8c\x06_names\x94}\x94\x8c\x07studies\x94K\x00N\x86\x94s\x8c\x12_allowed_overrides\x94]\x94(\x8c\x05index\x94\x8c\x04sort\x94eh\x12\x8c\tfunctools\x94\x8c\x07partial\x94\x93\x94h\x06\x8c\x19Namedlist._used_attribute\x94\x93\x94\x85\x94R\x94(h\x18)}\x94\x8c\x05_name\x94h\x12sNt\x94bh\x13h\x16h\x18\x85\x94R\x94(h\x18)}\x94h\x1ch\x13sNt\x94bh\x0eh\nub\x8c\x06output\x94h\x06\x8c\x0bOutputFiles\x94\x93\x94)\x81\x94\x8c8/d/docker/volumes/regep00/gen3/submissions/new/study.csv\x94a}\x94(h\x0c}\x94h\x0eK\x00N\x86\x94sh\x10]\x94(h\x12h\x13eh\x12h\x16h\x18\x85\x94R\x94(h\x18)}\x94h\x1ch\x12sNt\x94bh\x13h\x16h\x18\x85\x94R\x94(h\x18)}\x94h\x1ch\x13sNt\x94bh\x0eh&ub\x8c\x06params\x94h\x06\x8c\x06Params\x94\x93\x94)\x81\x94}\x94(h\x0c}\x94h\x10]\x94(h\x12h\x13eh\x12h\x16h\x18\x85\x94R\x94(h\x18)}\x94h\x1ch\x12sNt\x94bh\x13h\x16h\x18\x85\x94R\x94(h\x18)}\x94h\x1ch\x13sNt\x94bub\x8c\twildcards\x94h\x06\x8c\tWildcards\x94\x93\x94)\x81\x94\x8c\x03csv\x94a}\x94(h\x0c}\x94\x8c\x06suffix\x94K\x00N\x86\x94sh\x10]\x94(h\x12h\x13eh\x12h\x16h\x18\x85\x94R\x94(h\x18)}\x94h\x1ch\x12sNt\x94bh\x13h\x16h\x18\x85\x94R\x94(h\x18)}\x94h\x1ch\x13sNt\x94b\x8c\x06suffix\x94hFub\x8c\x07threads\x94K\x01\x8c\tresources\x94h\x06\x8c\tResources\x94\x93\x94)\x81\x94(K\x01K\x01M\xe8\x03M\xe8\x03\x8c\x19/d/q4/9885035.1.linux01.q\x94e}\x94(h\x0c}\x94(\x8c\x06_cores\x94K\x00N\x86\x94\x8c\x06_nodes\x94K\x01N\x86\x94\x8c\x06mem_mb\x94K\x02N\x86\x94\x8c\x07disk_mb\x94K\x03N\x86\x94\x8c\x06tmpdir\x94K\x04N\x86\x94uh\x10]\x94(h\x12h\x13eh\x12h\x16h\x18\x85\x94R\x94(h\x18)}\x94h\x1ch\x12sNt\x94bh\x13h\x16h\x18\x85\x94R\x94(h\x18)}\x94h\x1ch\x13sNt\x94bh]K\x01h_K\x01haM\xe8\x03hcM\xe8\x03hehZub\x8c\x03log\x94h\x06\x8c\x03Log\x94\x93\x94)\x81\x94}\x94(h\x0c}\x94h\x10]\x94(h\x12h\x13eh\x12h\x16h\x18\x85\x94R\x94(h\x18)}\x94h\x1ch\x12sNt\x94bh\x13h\x16h\x18\x85\x94R\x94(h\x18)}\x94h\x1ch\x13sNt\x94bub\x8c\x06config\x94}\x94\x8c\x04rule\x94\x8c\x07T_STUDY\x94\x8c\x0fbench_iteration\x94N\x8c\tscriptdir\x94\x8c+/udd/rexin/code/etl/workflow/scripts/python\x94ub.'); from snakemake.logging import logger; logger.printshellcmds = True; __real_file__ = __file__; __file__ = '/udd/rexin/code/etl/workflow/scripts/python/transform_studies.py';
######## snakemake preamble end #########
import logging
import pandas as pd
import uuid

GEN3_TYPE = 'study'
PROJECT_CODE = 'p0'
PROJECT_ID = 'g0-p0'
RENAME_COLUMNS = {
    's_studyid':     'submitter_id',
    'studydesc':     'study_description',
    'u_studydesign': 'study_design'
}

if __name__ == '__main__':

    study = pd.read_csv(snakemake.input.studies)
    study = study.rename(columns=RENAME_COLUMNS)

    # Define the column names to be added
    # rejpz: only add these if we are going to use them; if blank, just ignore
    #new_columns = ['associated_experiment', 'copy_numbers_identified', 'data_description', 'experimental_description',
    #               'experimental_intent', 'indels_identified', 'marker_panel_description', 'number_experimental_group',
    #               'number_samples_per_experimental_group', 'number_samples_per_experimental_group',
    #               'somatic_mutations_identified', 'type_of_data', 'type_of_sample', 'type_of_specimen']
    new_columns = []

    # Add the new columns with 'N/A' as the default value
    for column in new_columns:
        if column not in study:
            study[column] =  None

    # Add the new column named type
    study['type'] = GEN3_TYPE
    study['projects.code'] = PROJECT_CODE
    study['project_id'] = PROJECT_ID
    study['guid'] = study.apply(lambda x: uuid.uuid4(), axis=1)
    study['study_description'] = study['study_description'].fillna(study['submitter_id'])

    study = study[['submitter_id', 'study_description', 'study_design', 'type', 'projects.code', 'project_id','guid']]


    # Export the DataFrame as a TSV file
    if snakemake.output.studies.endswith('.tsv'):
        delimiter = '\t'
        study = study.drop(columns=['guid',])
    else:
        delimiter = ','

    study.to_csv(snakemake.output.studies, index=False, sep=delimiter)
