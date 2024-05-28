#!/bin/bash
# parse dir 
ENV_WORK_DIR=$(pwd)

cat <<EOF > .env
VIRTUAL_ENV="$ENV_WORK_DIR/.venv"
PYTHONPATH="$ENV_WORK_DIR/data-processing-spark"
EOF