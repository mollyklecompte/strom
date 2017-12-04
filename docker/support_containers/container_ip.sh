#!/bin/bash
useage="$(basename "$0") [-h] container_name config_name

where:
    --help show this help text
    container_name: the name of the container from docker-compse.yaml
    config_name: name of the value to set in your ~/.strom/strom.ini configuration file"

if [[ $# -eq 0 ]] ; then
    echo "$useage"
    exit 0
fi

while getopts ":h" opt; do
    case $opt in
    h)
        echo "$useage"
        exit 0
        ;;
    \?)
        echo "invalid option: -$OPTARG"
        exit 1
        ;;
    esac
done

container_name=$1
config_name=$2
container_ip=$(docker inspect supportcontainers_"$container_name"_1 | grep IPAddress\": | sed "s/[^0-9\.]*//g" | sed "/^$/d" | sed -n 1p)
sed "/^$config_name=/d" -i $HOME/.strom/strom.ini
echo $config_name"="$container_ip >> $HOME/.strom/strom.ini