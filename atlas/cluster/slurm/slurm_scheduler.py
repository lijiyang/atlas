#!/usr/bin/env python3
"""
Submit this clustering script for sbatch to snakemake with:

    snakemake -j 99 --cluster slurm_scheduler.py
"""

import os
import sys
import warnings
from subprocess import Popen, PIPE

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

from snakemake.utils import read_job_properties

jobscript = sys.argv[1]

job_properties = read_job_properties(jobscript)

cluster_param={}

job_resources= job_properties["resources"]


if not "mem" in job_resources:
    warnings.warn("Rule {rule} has no memory specified, set to default.".format(**job_properties))

# do something useful with the threads
cluster_param["threads"] = job_properties.get("threads",1)
cluster_param['time'] = job_resources.get("time",job_properties["cluster"].get('time',300))
cluster_param['mem'] = int(job_resources.get("mem",10))
cluster_param['name'] = job_properties['rule']

cluster_param['partition'] = job_properties["cluster"]['partition']
#cluster_param['account'] = job_properties["cluster"]['account']

#--account={account}

eprint("Submit job with parameters:\n"+"\n".join(["\t{} : {}".format(key,cluster_param[key]) for key in cluster_param]))
command= "sbatch -p {partition} --parsable -c {threads} --time={time} --mem={mem}g --job-name={name} {script}".format(script=jobscript, **cluster_param)
p = Popen(command.split(' '), stdout=PIPE, stderr=PIPE)
output, error = p.communicate()
if p.returncode != 0: 
   raise Exception("Job can't be submitted\n"+output.decode("utf-8")+error.decode("utf-8"))
else:
	print(output.decode("utf-8"))



