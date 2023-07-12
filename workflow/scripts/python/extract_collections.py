{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "07188561-b892-45c1-9fb1-e7db5722b92b",
   "metadata": {},
   "outputs": [
    {
     "ename": "NoSuchTableError",
     "evalue": "S_COLLECTIONS",
     "output_type": "error",
     "traceback": [
      "\u001b[0;31m---------------------------------------------------------------------------\u001b[0m",
      "\u001b[0;31mNoSuchTableError\u001b[0m                          Traceback (most recent call last)",
      "Cell \u001b[0;32mIn[1], line 13\u001b[0m\n\u001b[1;32m     10\u001b[0m Session \u001b[38;5;241m=\u001b[39m sessionmaker(bind\u001b[38;5;241m=\u001b[39mengine)\n\u001b[1;32m     11\u001b[0m session \u001b[38;5;241m=\u001b[39m Session()\n\u001b[0;32m---> 13\u001b[0m Collections \u001b[38;5;241m=\u001b[39m \u001b[43mTable\u001b[49m\u001b[43m(\u001b[49m\u001b[38;5;124;43m'\u001b[39;49m\u001b[38;5;124;43mS_COLLECTIONS\u001b[39;49m\u001b[38;5;124;43m'\u001b[39;49m\u001b[43m,\u001b[49m\u001b[43msapphire8_md\u001b[49m\u001b[43m,\u001b[49m\u001b[43mautoload\u001b[49m\u001b[38;5;241;43m=\u001b[39;49m\u001b[38;5;28;43;01mTrue\u001b[39;49;00m\u001b[43m,\u001b[49m\u001b[43mautoload_with\u001b[49m\u001b[38;5;241;43m=\u001b[39;49m\u001b[43mengine\u001b[49m\u001b[43m)\u001b[49m\n\u001b[1;32m     14\u001b[0m CollectionsFamily \u001b[38;5;241m=\u001b[39m Table(\u001b[38;5;124m'\u001b[39m\u001b[38;5;124mS_COLLECTIONSFAMILY\u001b[39m\u001b[38;5;124m'\u001b[39m,sapphire8_md,autoload\u001b[38;5;241m=\u001b[39m\u001b[38;5;28;01mTrue\u001b[39;00m,autoload_with\u001b[38;5;241m=\u001b[39mengine)\n\u001b[1;32m     16\u001b[0m \u001b[38;5;28;01mif\u001b[39;00m \u001b[38;5;18m__name__\u001b[39m \u001b[38;5;241m==\u001b[39m \u001b[38;5;124m'\u001b[39m\u001b[38;5;124m__main__\u001b[39m\u001b[38;5;124m'\u001b[39m:\n",
      "File \u001b[0;32m<string>:2\u001b[0m, in \u001b[0;36m__new__\u001b[0;34m(cls, *args, **kw)\u001b[0m\n",
      "File \u001b[0;32m~/.conda/envs/cdnm-jupyter-python-3.7.6/lib/python3.9/site-packages/sqlalchemy/util/deprecations.py:298\u001b[0m, in \u001b[0;36mdeprecated_params.<locals>.decorate.<locals>.warned\u001b[0;34m(fn, *args, **kwargs)\u001b[0m\n\u001b[1;32m    291\u001b[0m     \u001b[38;5;28;01mif\u001b[39;00m m \u001b[38;5;129;01min\u001b[39;00m kwargs:\n\u001b[1;32m    292\u001b[0m         _warn_with_version(\n\u001b[1;32m    293\u001b[0m             messages[m],\n\u001b[1;32m    294\u001b[0m             versions[m],\n\u001b[1;32m    295\u001b[0m             version_warnings[m],\n\u001b[1;32m    296\u001b[0m             stacklevel\u001b[38;5;241m=\u001b[39m\u001b[38;5;241m3\u001b[39m,\n\u001b[1;32m    297\u001b[0m         )\n\u001b[0;32m--> 298\u001b[0m \u001b[38;5;28;01mreturn\u001b[39;00m \u001b[43mfn\u001b[49m\u001b[43m(\u001b[49m\u001b[38;5;241;43m*\u001b[39;49m\u001b[43margs\u001b[49m\u001b[43m,\u001b[49m\u001b[43m \u001b[49m\u001b[38;5;241;43m*\u001b[39;49m\u001b[38;5;241;43m*\u001b[39;49m\u001b[43mkwargs\u001b[49m\u001b[43m)\u001b[49m\n",
      "File \u001b[0;32m~/.conda/envs/cdnm-jupyter-python-3.7.6/lib/python3.9/site-packages/sqlalchemy/sql/schema.py:600\u001b[0m, in \u001b[0;36mTable.__new__\u001b[0;34m(cls, *args, **kw)\u001b[0m\n\u001b[1;32m    598\u001b[0m \u001b[38;5;28;01mexcept\u001b[39;00m \u001b[38;5;167;01mException\u001b[39;00m:\n\u001b[1;32m    599\u001b[0m     \u001b[38;5;28;01mwith\u001b[39;00m util\u001b[38;5;241m.\u001b[39msafe_reraise():\n\u001b[0;32m--> 600\u001b[0m         metadata\u001b[38;5;241m.\u001b[39m_remove_table(name, schema)\n",
      "File \u001b[0;32m~/.conda/envs/cdnm-jupyter-python-3.7.6/lib/python3.9/site-packages/sqlalchemy/util/langhelpers.py:70\u001b[0m, in \u001b[0;36msafe_reraise.__exit__\u001b[0;34m(self, type_, value, traceback)\u001b[0m\n\u001b[1;32m     68\u001b[0m     \u001b[38;5;28mself\u001b[39m\u001b[38;5;241m.\u001b[39m_exc_info \u001b[38;5;241m=\u001b[39m \u001b[38;5;28;01mNone\u001b[39;00m  \u001b[38;5;66;03m# remove potential circular references\u001b[39;00m\n\u001b[1;32m     69\u001b[0m     \u001b[38;5;28;01mif\u001b[39;00m \u001b[38;5;129;01mnot\u001b[39;00m \u001b[38;5;28mself\u001b[39m\u001b[38;5;241m.\u001b[39mwarn_only:\n\u001b[0;32m---> 70\u001b[0m         \u001b[43mcompat\u001b[49m\u001b[38;5;241;43m.\u001b[39;49m\u001b[43mraise_\u001b[49m\u001b[43m(\u001b[49m\n\u001b[1;32m     71\u001b[0m \u001b[43m            \u001b[49m\u001b[43mexc_value\u001b[49m\u001b[43m,\u001b[49m\n\u001b[1;32m     72\u001b[0m \u001b[43m            \u001b[49m\u001b[43mwith_traceback\u001b[49m\u001b[38;5;241;43m=\u001b[39;49m\u001b[43mexc_tb\u001b[49m\u001b[43m,\u001b[49m\n\u001b[1;32m     73\u001b[0m \u001b[43m        \u001b[49m\u001b[43m)\u001b[49m\n\u001b[1;32m     74\u001b[0m \u001b[38;5;28;01melse\u001b[39;00m:\n\u001b[1;32m     75\u001b[0m     \u001b[38;5;28;01mif\u001b[39;00m \u001b[38;5;129;01mnot\u001b[39;00m compat\u001b[38;5;241m.\u001b[39mpy3k \u001b[38;5;129;01mand\u001b[39;00m \u001b[38;5;28mself\u001b[39m\u001b[38;5;241m.\u001b[39m_exc_info \u001b[38;5;129;01mand\u001b[39;00m \u001b[38;5;28mself\u001b[39m\u001b[38;5;241m.\u001b[39m_exc_info[\u001b[38;5;241m1\u001b[39m]:\n\u001b[1;32m     76\u001b[0m         \u001b[38;5;66;03m# emulate Py3K's behavior of telling us when an exception\u001b[39;00m\n\u001b[1;32m     77\u001b[0m         \u001b[38;5;66;03m# occurs in an exception handler.\u001b[39;00m\n",
      "File \u001b[0;32m~/.conda/envs/cdnm-jupyter-python-3.7.6/lib/python3.9/site-packages/sqlalchemy/util/compat.py:207\u001b[0m, in \u001b[0;36mraise_\u001b[0;34m(***failed resolving arguments***)\u001b[0m\n\u001b[1;32m    204\u001b[0m     exception\u001b[38;5;241m.\u001b[39m__cause__ \u001b[38;5;241m=\u001b[39m replace_context\n\u001b[1;32m    206\u001b[0m \u001b[38;5;28;01mtry\u001b[39;00m:\n\u001b[0;32m--> 207\u001b[0m     \u001b[38;5;28;01mraise\u001b[39;00m exception\n\u001b[1;32m    208\u001b[0m \u001b[38;5;28;01mfinally\u001b[39;00m:\n\u001b[1;32m    209\u001b[0m     \u001b[38;5;66;03m# credit to\u001b[39;00m\n\u001b[1;32m    210\u001b[0m     \u001b[38;5;66;03m# https://cosmicpercolator.com/2016/01/13/exception-leaks-in-python-2-and-3/\u001b[39;00m\n\u001b[1;32m    211\u001b[0m     \u001b[38;5;66;03m# as the __traceback__ object creates a cycle\u001b[39;00m\n\u001b[1;32m    212\u001b[0m     \u001b[38;5;28;01mdel\u001b[39;00m exception, replace_context, from_, with_traceback\n",
      "File \u001b[0;32m~/.conda/envs/cdnm-jupyter-python-3.7.6/lib/python3.9/site-packages/sqlalchemy/sql/schema.py:595\u001b[0m, in \u001b[0;36mTable.__new__\u001b[0;34m(cls, *args, **kw)\u001b[0m\n\u001b[1;32m    593\u001b[0m metadata\u001b[38;5;241m.\u001b[39m_add_table(name, schema, table)\n\u001b[1;32m    594\u001b[0m \u001b[38;5;28;01mtry\u001b[39;00m:\n\u001b[0;32m--> 595\u001b[0m     \u001b[43mtable\u001b[49m\u001b[38;5;241;43m.\u001b[39;49m\u001b[43m_init\u001b[49m\u001b[43m(\u001b[49m\u001b[43mname\u001b[49m\u001b[43m,\u001b[49m\u001b[43m \u001b[49m\u001b[43mmetadata\u001b[49m\u001b[43m,\u001b[49m\u001b[43m \u001b[49m\u001b[38;5;241;43m*\u001b[39;49m\u001b[43margs\u001b[49m\u001b[43m,\u001b[49m\u001b[43m \u001b[49m\u001b[38;5;241;43m*\u001b[39;49m\u001b[38;5;241;43m*\u001b[39;49m\u001b[43mkw\u001b[49m\u001b[43m)\u001b[49m\n\u001b[1;32m    596\u001b[0m     table\u001b[38;5;241m.\u001b[39mdispatch\u001b[38;5;241m.\u001b[39mafter_parent_attach(table, metadata)\n\u001b[1;32m    597\u001b[0m     \u001b[38;5;28;01mreturn\u001b[39;00m table\n",
      "File \u001b[0;32m~/.conda/envs/cdnm-jupyter-python-3.7.6/lib/python3.9/site-packages/sqlalchemy/sql/schema.py:670\u001b[0m, in \u001b[0;36mTable._init\u001b[0;34m(self, name, metadata, *args, **kwargs)\u001b[0m\n\u001b[1;32m    666\u001b[0m \u001b[38;5;66;03m# load column definitions from the database if 'autoload' is defined\u001b[39;00m\n\u001b[1;32m    667\u001b[0m \u001b[38;5;66;03m# we do it after the table is in the singleton dictionary to support\u001b[39;00m\n\u001b[1;32m    668\u001b[0m \u001b[38;5;66;03m# circular foreign keys\u001b[39;00m\n\u001b[1;32m    669\u001b[0m \u001b[38;5;28;01mif\u001b[39;00m autoload:\n\u001b[0;32m--> 670\u001b[0m     \u001b[38;5;28;43mself\u001b[39;49m\u001b[38;5;241;43m.\u001b[39;49m\u001b[43m_autoload\u001b[49m\u001b[43m(\u001b[49m\n\u001b[1;32m    671\u001b[0m \u001b[43m        \u001b[49m\u001b[43mmetadata\u001b[49m\u001b[43m,\u001b[49m\n\u001b[1;32m    672\u001b[0m \u001b[43m        \u001b[49m\u001b[43mautoload_with\u001b[49m\u001b[43m,\u001b[49m\n\u001b[1;32m    673\u001b[0m \u001b[43m        \u001b[49m\u001b[43minclude_columns\u001b[49m\u001b[43m,\u001b[49m\n\u001b[1;32m    674\u001b[0m \u001b[43m        \u001b[49m\u001b[43m_extend_on\u001b[49m\u001b[38;5;241;43m=\u001b[39;49m\u001b[43m_extend_on\u001b[49m\u001b[43m,\u001b[49m\n\u001b[1;32m    675\u001b[0m \u001b[43m        \u001b[49m\u001b[43mresolve_fks\u001b[49m\u001b[38;5;241;43m=\u001b[39;49m\u001b[43mresolve_fks\u001b[49m\u001b[43m,\u001b[49m\n\u001b[1;32m    676\u001b[0m \u001b[43m    \u001b[49m\u001b[43m)\u001b[49m\n\u001b[1;32m    678\u001b[0m \u001b[38;5;66;03m# initialize all the column, etc. objects.  done after reflection to\u001b[39;00m\n\u001b[1;32m    679\u001b[0m \u001b[38;5;66;03m# allow user-overrides\u001b[39;00m\n\u001b[1;32m    681\u001b[0m \u001b[38;5;28mself\u001b[39m\u001b[38;5;241m.\u001b[39m_init_items(\n\u001b[1;32m    682\u001b[0m     \u001b[38;5;241m*\u001b[39margs,\n\u001b[1;32m    683\u001b[0m     allow_replacements\u001b[38;5;241m=\u001b[39mextend_existing \u001b[38;5;129;01mor\u001b[39;00m keep_existing \u001b[38;5;129;01mor\u001b[39;00m autoload\n\u001b[1;32m    684\u001b[0m )\n",
      "File \u001b[0;32m~/.conda/envs/cdnm-jupyter-python-3.7.6/lib/python3.9/site-packages/sqlalchemy/sql/schema.py:705\u001b[0m, in \u001b[0;36mTable._autoload\u001b[0;34m(self, metadata, autoload_with, include_columns, exclude_columns, resolve_fks, _extend_on)\u001b[0m\n\u001b[1;32m    703\u001b[0m insp \u001b[38;5;241m=\u001b[39m inspection\u001b[38;5;241m.\u001b[39minspect(autoload_with)\n\u001b[1;32m    704\u001b[0m \u001b[38;5;28;01mwith\u001b[39;00m insp\u001b[38;5;241m.\u001b[39m_inspection_context() \u001b[38;5;28;01mas\u001b[39;00m conn_insp:\n\u001b[0;32m--> 705\u001b[0m     \u001b[43mconn_insp\u001b[49m\u001b[38;5;241;43m.\u001b[39;49m\u001b[43mreflect_table\u001b[49m\u001b[43m(\u001b[49m\n\u001b[1;32m    706\u001b[0m \u001b[43m        \u001b[49m\u001b[38;5;28;43mself\u001b[39;49m\u001b[43m,\u001b[49m\n\u001b[1;32m    707\u001b[0m \u001b[43m        \u001b[49m\u001b[43minclude_columns\u001b[49m\u001b[43m,\u001b[49m\n\u001b[1;32m    708\u001b[0m \u001b[43m        \u001b[49m\u001b[43mexclude_columns\u001b[49m\u001b[43m,\u001b[49m\n\u001b[1;32m    709\u001b[0m \u001b[43m        \u001b[49m\u001b[43mresolve_fks\u001b[49m\u001b[43m,\u001b[49m\n\u001b[1;32m    710\u001b[0m \u001b[43m        \u001b[49m\u001b[43m_extend_on\u001b[49m\u001b[38;5;241;43m=\u001b[39;49m\u001b[43m_extend_on\u001b[49m\u001b[43m,\u001b[49m\n\u001b[1;32m    711\u001b[0m \u001b[43m    \u001b[49m\u001b[43m)\u001b[49m\n",
      "File \u001b[0;32m~/.conda/envs/cdnm-jupyter-python-3.7.6/lib/python3.9/site-packages/sqlalchemy/engine/reflection.py:788\u001b[0m, in \u001b[0;36mInspector.reflect_table\u001b[0;34m(self, table, include_columns, exclude_columns, resolve_fks, _extend_on)\u001b[0m\n\u001b[1;32m    779\u001b[0m     \u001b[38;5;28mself\u001b[39m\u001b[38;5;241m.\u001b[39m_reflect_column(\n\u001b[1;32m    780\u001b[0m         table,\n\u001b[1;32m    781\u001b[0m         col_d,\n\u001b[0;32m   (...)\u001b[0m\n\u001b[1;32m    784\u001b[0m         cols_by_orig_name,\n\u001b[1;32m    785\u001b[0m     )\n\u001b[1;32m    787\u001b[0m \u001b[38;5;28;01mif\u001b[39;00m \u001b[38;5;129;01mnot\u001b[39;00m found_table:\n\u001b[0;32m--> 788\u001b[0m     \u001b[38;5;28;01mraise\u001b[39;00m exc\u001b[38;5;241m.\u001b[39mNoSuchTableError(table\u001b[38;5;241m.\u001b[39mname)\n\u001b[1;32m    790\u001b[0m \u001b[38;5;28mself\u001b[39m\u001b[38;5;241m.\u001b[39m_reflect_pk(\n\u001b[1;32m    791\u001b[0m     table_name, schema, table, cols_by_orig_name, exclude_columns\n\u001b[1;32m    792\u001b[0m )\n\u001b[1;32m    794\u001b[0m \u001b[38;5;28mself\u001b[39m\u001b[38;5;241m.\u001b[39m_reflect_fk(\n\u001b[1;32m    795\u001b[0m     table_name,\n\u001b[1;32m    796\u001b[0m     schema,\n\u001b[0;32m   (...)\u001b[0m\n\u001b[1;32m    802\u001b[0m     reflection_options,\n\u001b[1;32m    803\u001b[0m )\n",
      "\u001b[0;31mNoSuchTableError\u001b[0m: S_COLLECTIONS"
     ]
    }
   ],
   "source": [
    "import pandas as pd\n",
    "\n",
    "from sqlalchemy import create_engine, MetaData, Table\n",
    "from sqlalchemy.orm import sessionmaker\n",
    "\n",
    "DBURL = 'oracle://sapphire_r:sapphire_r0@oar1.bwh.harvard.edu:1521/labvprd'\n",
    "\n",
    "sapphire8_md = MetaData(schema='SAPPHIRE8')\n",
    "engine = create_engine(DBURL)\n",
    "Session = sessionmaker(bind=engine)\n",
    "session = Session()\n",
    "\n",
    "Collections = Table('S_COLLECTIONS',sapphire8_md,autoload=True,autoload_with=engine)\n",
    "CollectionsFamily = Table('S_COLLECTIONSFAMILY',sapphire8_md,autoload=True,autoload_with=engine)\n",
    "\n",
    "if __name__ == '__main__':\n",
    "    q_collections = session.query(Collection).filter()\n",
    "    collections = pd.read_sql(q_collections.statement, session.bind)\n",
    "    collections.drop(columns=['usersequence', 'auditsequence', 'tracelogid', 'timepointname', 'timepointstart', 'timepointend', 'timepointtimeunits', \n",
    "                          'visitstart',  'visitname', 'visittimeunits', 'visitend', 'alternatename', 'receivableflag', 'restrictionclassid',\n",
    "                         'protocolname', 'startdt', 'enddt', 'templateflag', 'notes', 'createtool', 'modtool'], inplace=True)\n",
    "    collections.to_csv(snakemake.output.df)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "10d596a4-8a55-41f8-a39f-c7758c51aab4",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "cdnm-jupyter-python-3.7.6",
   "language": "python",
   "name": "cdnm-jupyter-python-3.7.6"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.9.16"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
