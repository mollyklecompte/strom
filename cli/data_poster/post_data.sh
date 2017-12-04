#!/bin/bash
### Script to continuously push data to the Flask server ###

i="1"
while [ $i -lt 225 ]
do
echo $i
endi=$[$i+50]
poster_dstream=$(sed "s/\[{/{/g" ../../python/Strom/demo_data/demo_trip26.txt | sed "s/}\]/}/g" | sed "s/{\"stream_name\"\:/\n&/g" | sed -n "$i","$endi"p | tr "\n" " " | sed "s/\"/\'/g" | awk '{printf"\"[%s]\"",$0}')
echo '{"stream_data":'$poster_dstream'}' > chunk.tmp
curl -H "Content-Type: application/json" -X POST -d @chunk.tmp 127.0.0.1:5000/kafka/load
i=$(($[$i+50] % 250))
echo "take 5"
sleep 5
done
