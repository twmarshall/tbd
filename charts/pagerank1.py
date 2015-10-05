#!/usr/bin/env python
# a bar plot with errorbars
import matplotlib.pyplot as plt
import numpy as np
import string

graph_dir = "charts/"
N = 1
width = 0.2
ind = np.arange(N)
fig, ax = plt.subplots()

#Spark
sparkMeans = [2819.0]
sparkStd = [59.78]
rects1 = ax.bar(ind, sparkMeans, width/2, color='#cc0000', yerr=sparkStd, log=True)

# TDB
tdbMeans = [23291.67]
tdbStd = [552.23]
rects2 = ax.bar(ind + width, tdbMeans, width/2, color='#3c78d8', yerr=tdbStd, log=True)

# update 10
oneMeans = [65.33]
oneStd = [11.9]
rects3 = ax.bar(ind + width * 2, oneMeans, width/2, color='#6aa84f', yerr=oneStd, log=True)

# update 100
twoMeans = [459.0]
twoStd = [72.01]
rects4 = ax.bar(ind + width * 3, twoMeans, width/2, color='#e69138', yerr=twoStd, log=True)

# add some text for labels, title and axes ticks
#ax.set_xlabel('Chunk Size')
ax.set_ylabel('Time (seconds)')
ax.set_title('100k node graph, 5 iterations')
ax.set_xticks(ind+width * 10)
#ax.set_xticklabels( ('1', '10', '100') )
ax.set_xlim([-width/2, (N - 1) + 4 * width])
#ax.set_ylim(ymin=0)
ax.set_yscale("symlog")

ax.legend( (rects1[0], rects2[0], rects3[0], rects4[0]), ('Non-incremental', 'Initial Run', 'Update 10', 'Update 100') )

def autolabel(rects):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x()+rect.get_width()/2., 1.05*height, '%d'%int(height),
                ha='center', va='bottom')

autolabel(rects1)
autolabel(rects2)
autolabel(rects3)
autolabel(rects4)

#plt.show()
plt.ylim(ymin=0)

plt.savefig(graph_dir + 'updates.png')
