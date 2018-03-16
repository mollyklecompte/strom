#!/usr/bin/env bash

echo ">;-D"

PATH_TO_PYBOARD=$1
PATH_TO_COMBINED=$2

cd python/Strom
python pyboard_gps.py

python $PATH_TO_PYBOARD $PATH_TO_COMBINED | python strom/stdin_mqtt.py