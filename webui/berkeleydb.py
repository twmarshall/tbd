import matplotlib
# prevents pyplot from trying to connect to x windowing
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import string
import sys

webui_root = "webui/"

file = open(webui_root + 'berkeleydb.txt', 'r')
reads = []
writes = []
for line in file:
    split = string.split(line)
    reads.append(split[0])
    writes.append(split[1])

plt.plot(reads, label="reads")
plt.plot(writes, label="writes")
plt.ylabel('operations per second')
plt.legend()
plt.savefig(webui_root + "berkeleydb.png")
