#!/usr/bin/env python
import matplotlib
# prevents pyplot from trying to connect to x windowing
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import numpy as np
import string
import sys

graph_dir = "charts/"

# Load results
file = open("results.txt", 'r')
prop = []
naiveTotal = 0.0
runningTotal = 0.0
index = 0
for line in file:
    split = string.split(line)

    label = split[0]

    total = float(split[4]) / 1000
    if (label == "naive"):
        naiveTotal = total
    elif (label == "initial"):
        runningTotal = total
        prop.append(total)
    else:
        prop.append(total + runningTotal)
        runningTotal = runningTotal + total

naive = []
for i in range(0, len(prop)):
    naive.append((i + 1) * naiveTotal)

x = np.arange(len(prop))

fig, ax = plt.subplots()

fig.set_size_inches([8, 5])
plt.plot(x, naive, label="Non-incremental")
plt.plot(x, prop, label="ThomasDB")

labels = np.arange(len(prop), step=10)
ax.set_xlabel("Batch Sequence Number")
ax.set_xticklabels(labels)
ax.set_xticks(labels)
ax.xaxis.grid(True)

ax.set_ylabel('Time (seconds)')
ax.set_yticks(np.arange(401, step=100))

ax.legend()

if len(sys.argv) > 1:
    plt.savefig(graph_dir + sys.argv[1])
else:
    plt.savefig(graph_dir + "propagation.png")
