#! /bin/bash
#####################################################################################################
##       Process thredds.acces.log from apache2 server
##         ./decompress_archived_aemet_data.sh
#####################################################################################################
##  Author: Cristian Muñoz Mas, Juan Gabriel Fernández
##          cmunoz@socib.es, jfernandez@socib.es
##  Creation: 02 November 2017
##  Latest modification: 02 November 2017
#####################################################################################################
##      Explanation:
##   1.- Copies thredds.access.log files from apache server log directory to data/archive2/ directory
##   2.- Run python toolbox to process thredds.access.log files
#####################################################################################################

# ===== Globals ======
testing=true

# ===== Server and file names =====
if [ "$testing" = true ] ; then
  # For testing purposes use your desired local path
  PATH_TO_COPY_SCRIPT=/home/cmunoz/python_projects/processing-logs/scripts
  PATH_TO_PROCESSING_LOG=/home/cmunoz/python_projects/processing-logs/log/processing.log
  PATH_TO_ERROR_LOG=/home/cmunoz/python_projects/processing-logs/log/processing.error.log
  PATH_TO_PYTHON_PROGRAM=/home/cmunoz/python_projects/processing-logs/main.py
else
  PATH_TO_PROCESSING_LOG=/home/cmunoz/python_projects/processing-logs/log/processing.log
  PATH_TO_ERROR_LOG=/home/cmunoz/python_projects/processing-logs/log/processing.error.log
  PATH_TO_PYTHON_PROGRAM=/home/cmunoz/python_projects/processing-logs/main.py
fi

echo "[`date +%d-%b-%Y` `date +%T`] Start process"
$PATH_TO_COPY_SCRIPT/copy_thredds_access_logs_to_archive2.sh >> $PATH_TO_PROCESSING_LOG 2>> $PATH_TO_ERROR_LOG

if [ $? -eq 0 ]; then
  echo "[`date +%d-%b-%Y` `date +%T`] Files properly copyied."
  echo "[`date +%d-%b-%Y` `date +%T`] Executing processing-logs."
  workon processing_logs
  python $PATH_TO_PYTHON_PROGRAM
  if [ $? -eq 0 ]; then
    echo "[`date +%d-%b-%Y` `date +%T`] processing-logs succesfully called."
  else
    echo "[`date +%d-%b-%Y` `date +%T`] ERROR during processing-logs call." >&2
  fi

else
  echo "[`date +%d-%b-%Y` `date +%T`] ERROR during copy operation." >&2
fi
