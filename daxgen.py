#!/usr/bin/env python
import sys
import os
import csv
from Pegasus.DAX3 import *
from optparse import OptionParser

# API Documentation: http://pegasus.isi.edu/documentation

# Create an abstract dag
workflow = ADAG("CyberShake-BB")

# Executables
e_setup = Executable('bb-setup', arch='x86_64', installed=False)
e_setup.addPFN(PFN('file://' + os.getcwd() + '/bin/bb-setup'))
workflow.addExecutable(e_setup)

e_delete = Executable('bb-delete', arch='x86_64', installed=False)
e_delete.addPFN(PFN('file://' + os.getcwd() + '/bin/bb-delete'))
workflow.addExecutable(e_delete)

e_sgt = Executable('sgt-generator-synth', arch='x86_64', installed=False)
e_sgt.addPFN(PFN('file://' + os.getcwd() + '/bin/sgt-generator-synth'))
workflow.addExecutable(e_sgt)

e_ds = Executable('sgt-generator-synth', arch='x86_64', installed=True)
e_ds.addPFN(PFN('file:///project/projectdirs/m2187/scottcal/CyberShake/software/DirectSynth/direct_synth.py'))
workflow.addExecutable(e_ds)


# BB Setup Job
j_setup = Job('bb-setup')
j_setup.addProfile(Profile(Namespace.Pegasus, 'glite.arguments', '#SBATCH -p debug'))
j_setup.addProfile(Profile(Namespace.Pegasus, 'glite.arguments', '#SBATCH -N 1'))
j_setup.addProfile(Profile(Namespace.Pegasus, 'glite.arguments', '#SBATCH -C haswell'))
j_setup.addProfile(Profile(Namespace.Pegasus, 'glite.arguments', '#SBATCH -t 00:05:00'))
j_setup.addProfile(Profile(Namespace.Pegasus, 'glite.arguments', '#BB create_persistent name=csbb capacity=700GB access=striped type=scratch'))
workflow.addJob(j_setup)

# SGT_Generator
j_sgt = Job('sgt-generator-synth')
j_sgt.addProfile(Profile(Namespace.Pegasus, 'glite.arguments', '#SBATCH -p regular'))
j_sgt.addProfile(Profile(Namespace.Pegasus, 'glite.arguments', '#SBATCH -N 64'))
j_sgt.addProfile(Profile(Namespace.Pegasus, 'glite.arguments', '#SBATCH -C haswell'))
j_sgt.addProfile(Profile(Namespace.Pegasus, 'glite.arguments', '#SBATCH -t 05:00:00'))
j_sgt.addProfile(Profile(Namespace.Pegasus, 'glite.arguments', '#DW persistentdw name=csbb'))
workflow.addJob(j_sgt)

f_sgtx = File('fx.sgt')
f_sgty = File('fy.sgt')
f_sgtxh = File('fx.sgtheader')
f_sgtyh = File('fy.sgtheader')
j_sgt.uses(f_sgtx, link=Link.OUTPUT, transfer=False)
j_sgt.uses(f_sgty, link=Link.OUTPUT, transfer=False)
j_sgt.uses(f_sgtxh, link=Link.OUTPUT, transfer=False)
j_sgt.uses(f_sgtyh, link=Link.OUTPUT, transfer=False)

# DirectSynth
j_ds = Job('sgt-generator-synth')
j_ds.addProfile(Profile(Namespace.Pegasus, 'glite.arguments', '#SBATCH -p regular'))
j_ds.addProfile(Profile(Namespace.Pegasus, 'glite.arguments', '#SBATCH -N 64'))
j_ds.addProfile(Profile(Namespace.Pegasus, 'glite.arguments', '#SBATCH -C haswell'))
j_ds.addProfile(Profile(Namespace.Pegasus, 'glite.arguments', '#SBATCH -t 05:00:00'))
j_ds.addProfile(Profile(Namespace.Pegasus, 'glite.arguments', '#DW persistentdw name=csbb'))
workflow.addJob(j_sgt)

j_ds.uses(f_sgtx, link=Link.INPUT)
j_ds.uses(f_sgty, link=Link.INPUT)
j_ds.uses(f_sgtxh, link=Link.INPUT)
j_ds.uses(f_sgtyh, link=Link.INPUT)

# BB Delete
j_delete = Job('bb-delete')
j_delete.addProfile(Profile(Namespace.Pegasus, 'glite.arguments', '#SBATCH -p debug'))
j_delete.addProfile(Profile(Namespace.Pegasus, 'glite.arguments', '#SBATCH -N 1'))
j_delete.addProfile(Profile(Namespace.Pegasus, 'glite.arguments', '#SBATCH -C haswell'))
j_delete.addProfile(Profile(Namespace.Pegasus, 'glite.arguments', '#SBATCH -t 00:05:00'))
j_delete.addProfile(Profile(Namespace.Pegasus, 'glite.arguments', '#BB destroy_persistent name=csbb'))
workflow.addJob(j_setup)

# dependencies
workflow.depends(j_ds, j_sgt)
workflow.depends(j_delete, j_ds)

# Write the DAX to file
f = open(options.daxfile, "w")
workflow.writeXML(f)
f.close()
