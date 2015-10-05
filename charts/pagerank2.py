import matplotlib
# prevents pyplot from trying to connect to x windowing                                                                                                       
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import string
import sys

fig, ax = plt.subplots()

graph_dir = 'charts/'

def plotLine(title, start, inc, color):
    x = []
    y = []
    val = start
    for i in range(0, 40):
        x.append(i)
        y.append(val)
        val = val + inc

    plt.plot(x, y, label=title, color=color)

plotLine("Update 10", 23291.67, 65.33, '#6aa84f')
plotLine("Non-incremental", 2819.0, 2819.0, '#cc0000')

plt.xlabel("Update Batches")
plt.ylabel('Cumulative Time (ms)')

#ax.set_title("Effect of Input Size on Update Time")

plt.legend()
plt.savefig(graph_dir + 'pagerank2.png')
