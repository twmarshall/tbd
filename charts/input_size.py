import matplotlib
# prevents pyplot from trying to connect to x windowing                                                                                                       
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import string
import sys

fig, ax = plt.subplots()

graph_dir = 'charts/'

def plotLine(line, title):
    file = open(graph_dir + 'input_size_' + line + '.txt', 'r')
    x = []
    y = []
    for line in file:
        split = string.split(line)
        x.append(split[0])
        y.append(split[1])

    plt.plot(x, y, label=title)

plotLine("initial_run", "Initial Run")
plotLine("update_1", "Update 100")
plotLine("spark", "Spark")

plt.xlabel("Input Size (GB)")
plt.ylabel('Time (seconds)')

#ax.set_title("Effect of Input Size on Update Time")

plt.legend()
plt.savefig(graph_dir + 'input_size.png')
