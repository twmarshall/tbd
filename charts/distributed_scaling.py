#!/usr/bin/env python
# a bar plot with errorbars
import matplotlib.pyplot as plt
import numpy as np
import string

graph_dir = "charts/"
N = 1
width = 0.01       # the width of the bars
ind = np.arange(N)  # the x locations for the groups
fig, ax = plt.subplots()

def readFile(file):
    file = open(graph_dir + file, 'r')
    means = []
    std = []
    for line in file:
        split = string.split(line)
        means.append(float(split[0]))
        #std.append(float(split[1]))
    return (means, std)

# initial run
(tdbMeans, tdbStd) = readFile("scaling_tdb.txt")
tdbMeans = [1.557289895]
rects1 = ax.bar(ind, tdbMeans, width, color='#6aa84f')

# update 10
#oneMeans= [0.3277027027]
oneMeans = [0]
rects2 = ax.bar(ind+width, oneMeans, width, color='#3c78d8')

# update 100
#twoMeans = [0.9172152797]
twoMeans = [0]
rects3 = ax.bar(ind+width*2, twoMeans, width, color='#e69138')

# update 1000
#threeMeans = [1.136780069]
threeMeans = [0]
rects4 = ax.bar(ind+width*3, threeMeans, width, color='#f1c232')

fontsize = '22'
# add some text for labels, title and axes ticks
ax.set_xlabel('Machines', fontsize=fontsize)
ax.set_xlim([-width, (N - 1) + 5 * width])
ax.set_ylabel('Speedup', fontsize=fontsize)
ax.set_title('Distributed', fontsize=fontsize)
ax.set_xticks(ind+width * 2)
ax.set_xticklabels( ('2'))

#ax.legend( (rects1[0], rects2[0], rects3[0], rects4[0]), ('Initial Run', 'Update 10', 'Update 100', 'Update 1000'), loc='best' )
#ax.legend( (rects1[0], rects2[0], rects3[0]), ('Initial Run', 'Update 10', 'Update 100'), loc='best' )
#ax.legend( (rects1[0], rects2[0]), ('Initial Run', 'Update 10'), loc='best' )
ax.legend( (rects1[0],), ('Initial Run',), loc='best' )

#plt.show()
plt.gcf().set_size_inches(6, 10)
plt.savefig(graph_dir + 'distributed_scaling.png')
