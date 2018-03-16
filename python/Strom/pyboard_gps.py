from strom.strom import Strom

s = Strom('http://localhost:5000/api')
s.create_strom(
    "test_stream",
    "device_id",
    [
        ([("location", "geo")],
         [],
         [],
         [])
    ],
    ["device_id"],
    [],
    "for the purpose of demo",
    source_mapping_list=[0,2,3],
    dstream_mapping_list=[
        ["user_ids", "device_id"],
        ["timestamp"],
        ["measures", "location", "val"]
    ],
    date_format='%Y-%m-%d-%H:%M:%S.%f',
    puller=[
        'mqtt',
        [
            ['uid', 'testerino'],
            ['data_format', 'csv']
        ]
    ]
)