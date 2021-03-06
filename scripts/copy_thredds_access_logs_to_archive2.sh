#! /bin/bash

# ===== Globals ======
testing=true

# ===== Server and file names =====
if [ "$testing" = true ] ; then
  # For testing purposes use your desired local path
  PATH_TO_LOGS=/home/cmunoz/Documents/programming/PythonScripts/processing-logs/test/
  LOG_FILE=thredds.socib.es.access.log
  PATH_TO_ARCHIVE=/home/cmunoz/Documents/programming/PythonScripts/processing-logs/data/input_logs/
else
  PATH_TO_LOGS=/var/log/apache2/
  LOG_FILE=thredds.socib.es.access.log
  PATH_TO_ARCHIVE=/data/archive2/logsArchive/thredds.access/rawInput/
fi

# Copy files from apache thredds.access.logs to data archive2 directory
echo "[`date +%d-%b-%Y` `date +%T`] Copying files at ${PATH_TO_ARCHIVE}"
for LOG_NUM in 0 1 2 3 4 5
do 
  if [ $LOG_NUM -eq 0 ]; then
    cp -rf $PATH_TO_LOGS$LOG_FILE $PATH_TO_ARCHIVE
  elif [ $LOG_NUM -eq 1 ]; then
    cp -rf $PATH_TO_LOGS$LOG_FILE.1 $PATH_TO_ARCHIVE
  else
    cp -rf $PATH_TO_LOGS$LOG_FILE.$LOG_NUM.gz $PATH_TO_ARCHIVE
  fi

  if [ $? -eq 0 ]; then
    echo "[`date +%d-%b-%Y` `date +%T`] Files properly copyied."
  else
    echo "[`date +%d-%b-%Y` `date +%T`] ERROR copying file $LOG_FILE.$LOG_NUM." >&2
    exit 1
  fi
done







