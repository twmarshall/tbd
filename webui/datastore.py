import matplotlib
# prevents pyplot from trying to connect to x windowing
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import string
import sys

webui_root = "webui/"

file = open(webui_root + 'datastore.txt', 'r')
reads = []
writes = []
creates = []
for line in file:
    split = string.split(line)
    reads.append(split[0])
    writes.append(split[1])
    creates.append(split[2])

readHandle = plt.plot(reads, label="reads")
writeHandle = plt.plot(writes, label="writes")
plt.plot(creates, label="creates")
plt.ylabel('operations per second')
plt.legend()
plt.savefig(webui_root + "datastore.png")
