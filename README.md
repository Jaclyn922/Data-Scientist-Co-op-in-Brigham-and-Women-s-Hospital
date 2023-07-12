# etl
Extract-Transform-Load code for building or maintaining the Gen3 Data Commons

## Running the Snakemake Workflow Version

```
ssh nantucket
/bin/bash
source /proj/relibs/relib00/conda-cdnm/bin/activate
conda activate sm7
conda config --set channel_priority strict
cat .condarc
snakemake -s workflow/Snakefile \
    --printshellcmds \
    --cluster="qsub -v PATH -cwd -o . -e . -l lx7 -terse -S /bin/bash" \
    --cluster-cancel="qdel" \
    --latency-wait=120 \
    --use-conda \
    --conda-prefix=/proj/relibs/relib00/smk-conda-cache/envs/ \
    --reason \
    --jobs=1 \
```
