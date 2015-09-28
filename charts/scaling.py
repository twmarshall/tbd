#!/usr/bin/env python
# a bar plot with errorbars
import matplotlib.pyplot as plt
import numpy as np
import string

graph_dir = "charts/"
N = 4
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

(tdbMeans, tdbStd) = readFile("scaling_tdb.txt")
rects1 = ax.bar(ind, tdbMeans, width, color='r', yerr=tdbStd)

#Spark

sparkMeans = []
sparkStd = []

(sparkMeans, sparkStd) = readFile("scaling_spark.txt")

rects2 = ax.bar(ind+width, sparkMeans, width, color='y', yerr=sparkStd)

# add some text for labels, title and axes ticks
ax.set_xlabel('Machines')
ax.set_ylabel('Updates/Second')
ax.set_title('Scalability')
ax.set_xticks(ind+width)
ax.set_xticklabels( ('1', '2', '3', '4'))

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
plt.savefig(graph_dir + 'scaling.png')
