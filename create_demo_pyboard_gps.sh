#!/usr/bin/env bash

echo ">;-D"

PATH_TO_PYBOARD=$1
PATH_TO_COMBINED=$2

cd python/Strom
python pyboard_gps.py
cd ../../

python $PATH_TO_PYBOARD $PATH_TO_COMBINED | python python/Strom/strom/stdin_mqtt.py
