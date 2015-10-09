#!/usr/bin/env python
# a bar plot with errorbars
import matplotlib.pyplot as plt
import numpy as np
import string

graph_dir = "charts/"
N = 5
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
        #std.append(float(split[1]))
    return (means, std)

# initial run
(tdbMeans, tdbStd) = readFile("scaling_tdb.txt")
tdbMeans = [1, 1.769826753, 2.81357155, 3.532328733, 5.035140782]
rects1 = ax.bar(ind, tdbMeans, width, color='#3c78d8')

# update 10
oneMeans= [1, 1.233837573, 1.190185049, 1.505154639, 1.724341561]
#oneMeans = [0, 0, 0, 0, 0]
rects2 = ax.bar(ind+width, oneMeans, width, color='#6aa84f')

# update 100
twoMeans = [1, 1.306933448, 1.643718385, 1.761395016, 2.220612885]
#twoMeans = [0, 0, 0, 0, 0]
rects3 = ax.bar(ind+width*2, twoMeans, width, color='#e69138')

# add some text for labels, title and axes ticks
ax.set_xlabel('Cores')
ax.set_xlim([-width, (N - 1) + 4 * width])
ax.set_ylabel('Speedup')
#ax.set_ylim([0, 5])
ax.set_title('Multi-core')
ax.set_xticks(ind+width * 1.5)
ax.set_xticklabels( ('1', '2', '4', '8', '12'))

#ax.legend( (rects1[0],), ('Initial Run',), loc='upper left' )
#ax.legend( (rects1[0], rects2[0]), ('Initial Run', 'Update 10'), loc='upper left' )
ax.legend( (rects1[0], rects2[0], rects3[0]), ('Initial Run', 'Update 10', 'Update 100'), loc='upper left' )

def autolabel(rects):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x()+rect.get_width()/2, height + .1, '%.2f'%float(height),
                ha='center', va='bottom', fontsize='12')

#autolabel(rects1)
#autolabel(rects2)
#autolabel(rects3)

#plt.show()
plt.savefig(graph_dir + 'scaling.png')
