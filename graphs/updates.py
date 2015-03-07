#!/usr/bin/env python
# a bar plot with errorbars
import matplotlib.pyplot as plt
import numpy as np
import string

graph_dir = "graphs/"
N = 5
width = 0.35       # the width of the bars
ind = np.arange(N)  # the x locations for the groups
fig, ax = plt.subplots()

# TDB
def readFile(file):
    file = open(graph_dir + file, 'r')
    means = []
    std = []
    for line in file:
        split = string.split(line)
        means.append(float(split[0]))
        std.append(float(split[1]))
    return (means, std)

(tdbMeans, tdbStd) = readFile("updates_tdb.txt")
rects1 = ax.bar(ind, tdbMeans, width, color='r', yerr=tdbStd)

#Spark

sparkMeans = []
sparkStd = []

(sparkMeans, sparkStd) = readFile("updates_spark.txt")

rects2 = ax.bar(ind+width, sparkMeans, width, color='y', yerr=sparkStd)

# add some text for labels, title and axes ticks
ax.set_ylabel('Time (seconds)')
ax.set_title('Time to Process Updates')
ax.set_xticks(ind+width)
ax.set_xticklabels( ('Initial Run', 'Update 1', 'Update 10', 'Update 100', 'update 1000') )

ax.legend( (rects1[0], rects2[0]), ('TDB', 'Spark') )

def autolabel(rects):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x()+rect.get_width()/2., 1.05*height, '%d'%int(height),
                ha='center', va='bottom')

autolabel(rects1)
autolabel(rects2)

#plt.show()
plt.savefig(graph_dir + 'updates.png')
