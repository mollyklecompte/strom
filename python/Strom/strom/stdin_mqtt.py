import sys

from mqtt.client import generate_message, config, publish

k = 0
# try:
#     buff = ''
#     while True:
#         buff += sys.stdin.read(1)
#         if buff.endswith('\n'):
#             print("buff:", buff[:-1])
#             buff = ''
#             k += 1
# except KeyboardInterrupt:
#    sys.stdout.flush()
#    pass
# print(k)

try:
    while True:
        buff = sys.stdin.readline()
        print(buff.rstrip())
        msg = generate_message(buff.rstrip().split(","), **config)
        publish([msg], **config)
        k += 1
except KeyboardInterrupt:
   sys.stdout.flush()
   pass
print(k)
