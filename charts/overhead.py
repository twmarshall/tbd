#!/usr/bin/env python
# a bar plot with errorbars
import matplotlib.pyplot as plt
import numpy as np
import string

graph_dir = "charts/"
N = 3
width = 0.2       # the width of the bars
ind = np.arange(N)  # the x locations for the groups
fig, ax = plt.subplots()

def readFile(file):
    file = open(graph_dir + file, 'r')
    means = []
    std = []
    for line in file:
        split = string.split(line)
        means.append(float(split[0]))
        std.append(float(split[1]))
    return (means, std)

(means, std) = readFile("overhead.txt")
rects1 = ax.bar(ind, means, width, color='#3c78d8', yerr=std)

# add some text for labels, title and axes ticks
ax.set_xlabel('Chunk Size')
ax.set_ylabel('Overhead')
ax.set_title('Storage Overhead')
ax.set_xticks(ind+width / 2)
ax.set_xticklabels( ('1', '10', '100') )
ax.set_xlim([-width, (N - 1) + 2 * width])

#ax.legend( (rects1[0]), ('TDB', 'Non-incremental', 'Update 10') )

def autolabel(rects):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x()+rect.get_width()/2., 1.05*height, '%d'%int(height),
                ha='center', va='bottom')

#autolabel(rects1)
#autolabel(rects2)

#plt.show()
plt.savefig(graph_dir + 'overhead.png')
