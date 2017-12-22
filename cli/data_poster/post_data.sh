#!/bin/bash
### Script to continuously push data to the Flask server ###
rm -f template.tmp stream_token.tmp
template_data=$(sed "s/\"/\\\\\"/g" ../../python/Strom/demo_data/demo_template.txt | awk '{printf"\"%s\"",$0}')
echo '{"template":'$template_data'}'  > template.tmp
curl -H "Content-Type: application/json" -X POST -d @template.tmp 127.0.0.1:5000/api/define > stream_token.tmp
kafka_topic=$(awk -F"engine_rules" '{print $2}' ../../python/Strom/demo_data/demo_template.txt | awk -F"kafka" '{print $2}' | sed "s/[^a-zA-Z0-9\-\_]//g" | awk '{printf"\"%s\"",$0}')
st=$(cat stream_token.tmp)
echo $st
i="1"
counter=1
sleep 5
while [ $i -lt 245 ]
do
echo $i $counter
endi=$[$i+10]

poster_dstream=$(cat ../../python/Strom/demo_data/demo_trip26.txt | cut -c 2-  | rev | cut -c 2-  | rev | sed "s/{\"stream_name\"\:/\n&/g" | sed -n "$i","$endi"p | tr "\n" " " | sed "s/,\s\+$//" | sed "s/\"/'/g" |sed "s/'stream_token': 'abc123'/'stream_token': '$st'/g" | sed "s/'/\\\\\"/g" | awk '{printf"\"[%s]\"",$0}')
echo '{"stream_data":'$poster_dstream', "topic":'$kafka_topic'}' > chunk.tmp
curl -H "Content-Type: application/json" -X POST -d @chunk.tmp 127.0.0.1:5000/kafka/load
i=$(($[$i+10] % 250))
((counter++))
sleep 0.1
done
