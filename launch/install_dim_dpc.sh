#!/bin/bash

TASKS_DIR_NAME="${HOME}/Tasks"
ROLLOUT_DIR_NAME="${HOME}"
PROJECT_NAME="agg_home_job_monthly"
PROJECT_DIR="${TASKS_DIR_NAME}/${PROJECT_NAME}"

mkdir -p ${PROJECT_DIR}

unzip ${ROLLOUT_DIR_NAME}/${PROJECT_NAME}.zip -d ${PROJECT_DIR}