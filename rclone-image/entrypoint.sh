#!/bin/bash

trap "exit 0" SIGINT SIGTERM;

if [ "${GIT_SYNC_MAX_SYNC_FAILURES}" = "FAIL_ON_ERROR" ];
then
  set -e
fi

if [ "${GIT_SYNC_REPO}" = "urlzip" ];
then
  if [ "${GIT_SYNC_ONE_TIME}" = "true" ];
  then
    echo "[$(date "+%Y-%m-%d %H:%M:%S")] Init, not sleeping"
    rclone copyurl "${GIT_SYNC_REV}" /data/dags.zip --log-level "${GITSYNC_VERBOSE:-NOTICE}"
    echo "[$(date "+%Y-%m-%d %H:%M:%S")] URL downloaded"
    unzip /data/dags.zip -d /unpacked_dags -o
    echo "[$(date "+%Y-%m-%d %H:%M:%S")] DAG archive unpacked"
    rclone sync /unpacked_dags/* /git/
    echo "[$(date "+%Y-%m-%d %H:%M:%S")] DAGs copied to DAG volume"
  else
    while :
    do
      echo "[$(date "+%Y-%m-%d %H:%M:%S")] sleeping ${GITSYNC_PERIOD} sec before downloading URL"
      sleep "${GITSYNC_PERIOD}"
      rclone copyurl "${GIT_SYNC_REV}" /data/dags.zip --log-level "${GITSYNC_VERBOSE:-NOTICE}"
      echo "[$(date "+%Y-%m-%d %H:%M:%S")] URL downloaded"
      unzip /data/dags.zip -d /unpacked_dags -o
      echo "[$(date "+%Y-%m-%d %H:%M:%S")] DAG archive unpacked"
      rclone sync /unpacked_dags/* /git/
      echo "[$(date "+%Y-%m-%d %H:%M:%S")] DAGs copied to DAG volume"
    done
  fi
else
  if [ "${GIT_SYNC_ONE_TIME}" = "true" ];
  then
    echo "[$(date "+%Y-%m-%d %H:%M:%S")] Init, not sleeping"
    rclone sync remotesrc:"${GIT_SYNC_REV}" /git --log-level "${GITSYNC_VERBOSE:-NOTICE}"
    echo "[$(date "+%Y-%m-%d %H:%M:%S")] sync done"
  else
    while :
    do
      echo "[$(date "+%Y-%m-%d %H:%M:%S")] sleeping ${GITSYNC_PERIOD} sec before sync"
      sleep "${GITSYNC_PERIOD}"
      echo "[$(date "+%Y-%m-%d %H:%M:%S")] syncing"
      rclone sync remotesrc:"${GIT_SYNC_REV}" /git --log-level "${GITSYNC_VERBOSE:-NOTICE}"
      echo "[$(date "+%Y-%m-%d %H:%M:%S")] sync done"
    done
  fi
fi
