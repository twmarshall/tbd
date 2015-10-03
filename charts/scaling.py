#!/usr/bin/env python
# a bar plot with errorbars
import matplotlib.pyplot as plt
import numpy as np
import string

graph_dir = "charts/"
N = 4
width = 0.20       # the width of the bars
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

# initial run
(tdbMeans, tdbStd) = readFile("scaling_tdb.txt")
rects1 = ax.bar(ind, tdbMeans, width, color='#6aa84f', yerr=tdbStd)

# update 10
(oneMeans, oneStd) = readFile("scaling_10.txt")
rects2 = ax.bar(ind+width, oneMeans, width, color='#3c78d8', yerr=oneStd)

# update 100
(twoMeans, twoStd) = readFile("scaling_100.txt")
rects3 = ax.bar(ind+width*2, twoMeans, width, color='#e69138', yerr=twoStd)

# add some text for labels, title and axes ticks
ax.set_xlabel('Machines')
ax.set_xlim([-width, (N - 1) + 4 * width])
ax.set_ylabel('Seconds')
#ax.set_ylim([0, 5])
ax.set_title('Scalability')
ax.set_xticks(ind+width * 1.5)
ax.set_xticklabels( ('1', '2', '3', '4'))

ax.legend( (rects1[0], rects2[0], rects3[0]), ('Initial Run', 'Update 10', 'Update 100') )

def autolabel(rects):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x()+rect.get_width()/2., 1.05*height, '%d'%int(height),
                ha='center', va='bottom')

#autolabel(rects1)
#autolabel(rects2)

#plt.show()
plt.savefig(graph_dir + 'scaling.png')
