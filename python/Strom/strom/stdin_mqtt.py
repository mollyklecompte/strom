import sys

from mqtt.client import generate_message, config, publish

k = 0

try:
    while True:
        buff = sys.stdin.readline()
        buff = buff.rstrip().split(",")
        if len(buff) == 5:
            tmp = buff.pop(-1)
            tmp2 = buff.pop(-1)
            buff.append((tmp2,tmp))
            print(buff)
            msg = generate_message(buff, **config)
            publish([msg], **config)
        else:
            print(buff)
        k += 1
except KeyboardInterrupt:
   sys.stdout.flush()
   pass
print(k)
