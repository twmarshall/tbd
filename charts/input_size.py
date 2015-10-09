import matplotlib
# prevents pyplot from trying to connect to x windowing                                                                                                       
#matplotlib.use('Agg')

import matplotlib.pyplot as plt
import string
import sys

fig, ax = plt.subplots()

graph_dir = 'charts/'

def plotLine(line, title, color):
    file = open(graph_dir + 'input_size_' + line + '.txt', 'r')
    x = []
    y = []
    for line in file:
        split = string.split(line)
        x.append(split[0])
        y.append(split[1])

    plt.plot(x, y, label=title, color=color)

plotLine("spark", "Non-Incremental", "#cc0000")
plotLine("initial_run", "Initial Run", '#3c78d8')
plotLine("update_1", "Update 1", '#6aa84f')

plt.xlabel("Input Size (# of pages)")
plt.ylabel('Time (ms)')

#ax.set_title("Effect of Input Size on Update Time")

plt.legend()
#plt.show()
plt.savefig(graph_dir + 'input_size.png')
