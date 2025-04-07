#
# gen.py [inputFile] [run/build]
#
#  from the list of files, generate commands to :
#    run PEAR
#    run geneEdit.py
#
#12-19-2019 - addded through git
from subprocess import call
from sys import argv
import csv
import os
import time
import argparse
#
# command strings
#
slurm1 = "#!/bin/bash"
slurm2 = "#SBATCH --job-name="
slurm3 = "#SBATCH --mail-type=FAIL       # Mail events (NONE, BEGIN, END, FAIL, ALL)"
slurm5 = "#SBATCH --ntasks=1                   # Run a single task"

slurm8 = "#SBATCH --output="
slurm9 = "date;hostname;pwd"


#-----------------------------------------------------------------------------------------------
# main start
#-----------------------------------------------------------------------------------------------
parser = argparse.ArgumentParser(description='Build script files to run gene editing pipeline')
parser.add_argument('inputFile',type=str,help='control file specifying input sequencing files and editing detials')
parser.add_argument('userName',type=str,help='Mail ID to receive slurm notifications')
parser.add_argument('--run',
                    help='run the scripts',
                    dest='runScript',
                    default=False,
                    action='store_const',
                    const=True)
parser.add_argument('--nopear',
                    help='do not run pear',
                    dest='runPear',
                    default=True,
                    action='store_const',
                    const=False)
parser.add_argument('--nofilter',
                    help='do not run geneEditFilter',
                    dest='runFilter',
                    default=True,
                    action='store_const',
                    const=False)
parser.add_argument('--nooutput',
                    help='do not run geneEditOutput',
                    dest='runOutput',
                    default=True,
                    action='store_const',
                    const=False)


args = parser.parse_args()
inputFile = args.inputFile
execute = args.runScript
runPear = args.runPear
runFilter = args.runFilter
runOutput = args.runOutput
userName = args.userName
#buildslurm username
slurm4 = "#SBATCH --mail-user={}@fredhutch.org".format(userName)
#ge_base   = "/home/{}/kiemlab/PROJECTS/Gene_Editing/".format(userName)
ge_base   = "/fh/fast/kiem_h/grp/kiemlab/PROJECTS/Gene_Editing/"
#
# read files to process
#
files = {}
#
# read input file
#
resultDir = ""
sourceDir = ""
with open(inputFile) as csv_file:
  csv_reader = csv.reader(csv_file, delimiter=',')
  for row in csv_reader:
    if row[0][0] == '#':
      pass
    elif row[0] == "SOURCE_DIR":
        sourceDir = row[1] + "/"
    elif row[0] == "RESULT_DIR":
        resultDir = row[1] + "/"
    else:
      key = row[0]
      files[key] = row
#
# verify
#
if len(files) == 0:
  print('could not read file ' + inputFile)
  exit(0)

if sourceDir == "":
  print("could not set Source Dir")
  print("there should be a line in the file 'SOURCE_DIR,XXXX  for dir under Gene_Editing'")
  exit(0)

if resultDir == "":
  print("could not set Result Dir")
  print("there should be a line in the file 'RESULT_DIR,XXXXX'")
  exit(0)
#
# make sure normal output dirs exist
#
if not os.path.isdir(ge_base + sourceDir + "aligned_reads/"):
  os.mkdir(ge_base + sourceDir + "aligned_reads/")
if not os.path.isdir(ge_base + sourceDir + "results/"):
  os.mkdir(ge_base + sourceDir + "results/")
#
# fix destination directories based on subject
#
#
# make sure batch,log and output directories exist
#
#
#
# convert each line into one batch file to run the job
#
for testRow in files.values():
  test = testRow[0]
  primerSet = testRow[1]
  outputBase = testRow[2]
  job_name     = test
  file_assembled = test + ".assembled"
  dir_base       = ge_base + sourceDir
  #file_r1        = ge_base + sourceDir + "fastq_files/" + test + "_1_150bp_4_lanes.merge.fastq"      # Base file name change for Vancouver sequencing.
  #file_r2        = ge_base + sourceDir + "fastq_files/" + test + "_2_150bp_4_lanes.merge.fastq"
  file_r1        = ge_base + sourceDir + "fastq_files/" + test + "_R1_001.fastq"
  file_r2        = ge_base + sourceDir + "fastq_files/" + test + "_R2_001.fastq"
  #file_r1        = ge_base + sourceDir + "fastq_files/" + test + "_R1.fastq"
  #file_r2        = ge_base + sourceDir + "fastq_files/" + test + "_R2.fastq"
  file_p_out     = ge_base + sourceDir + "stitched_reads/" + test
  file_script    = test + '_script.sh'
  file_log       = test + '_log.txt'
  file_ge        = ge_base + 'scripts/' + 'geneEditFilter.py'
  file_geOut     = ge_base + 'scripts/' + 'geneEditOutput.py'
  #
  #
  #
  with open(file_script,'w') as sh:
    print("creating script file {}".format(file_script))
    #
    # SLURM
    #
    sh.write(slurm1+'\n')
    sh.write(slurm2+job_name)
    sh.write(slurm3+'\n')
    sh.write(slurm4+'\n')
    sh.write(slurm5+'\n')
    # runFilter uses multi-processes
    if runFilter:
      sh.write("#SBATCH --cpus-per-task=8\n")
      sh.write("#SBATCH --mem=200000\n")
      sh.write("#SBATCH --partition=campus-new\n")
    else:
      sh.write("#SBATCH --cpus-per-task=1\n")

    sh.write(slurm8+file_log+'\n')
    sh.write(slurm9+'\n')
    #---------------------------------------------------------------------------
    # PEAR
    #---------------------------------------------------------------------------
    if runPear:
      sh.write("pear -f " + file_r1 + " \\\n")
      sh.write("-r " + file_r2 + " \\\n")
      sh.write("-o " + file_p_out + "\n")
    #---------------------------------------------------------------------------
    # python filter script
    #---------------------------------------------------------------------------
    if runFilter:
      sh.write("python " + file_ge + " \\\n")
      sh.write(dir_base + " " + file_assembled + " \\\n")
      sh.write(resultDir + " " + outputBase + " " + primerSet + '\n')
    #---------------------------------------------------------------------------
    # python output script
    #---------------------------------------------------------------------------
    if runOutput:
      sh.write("python " + file_geOut + " \\\n")
      sh.write(dir_base + " " + file_assembled + " \\\n")
      sh.write(resultDir + " " + outputBase + " " + primerSet + '\n')
  #
  # execute
  #
  if execute == True:
      while True:
        a = os.popen("squeue -u {}".format(userName)).read()
        print(a)
        b = len([char for char in a if char == '\n'])
        if b <= 6:break
        time.sleep(15)

  if execute == True:
    print("Start execution of " + file_script)
    call(["sbatch",file_script])
