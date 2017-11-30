#!/bin/bash
### Script to continuously push data to the Flask server ###

i="1"
while [ $i -lt 200 ]
do
echo $i
endi=$[$i+49]
poster_dstream=$(sed "s/\[{/{/g" ../../python/Strom/demo_data/demo_trip26.txt | sed "s/}\]/}/g" | sed "s/{\"stream_name\"\:/\n&/g" | sed -n "$i","$endi"p | tr "\n" " " | awk '{print "{\"stream_data\":["$0"]}"}')
curl -X POST -d "$poster_dstream" 127.0.0.1:5000/kafka/load
i=$(($[$i+50] % 200))
echo "take 5"
sleep 5
done
