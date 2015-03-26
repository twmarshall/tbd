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
file = open(graph_dir + "updates2.txt", 'r')
load = []
gc = []
rest = []
std = []
for line in file:
    split = string.split(line)
    load.append(float(split[0]))
    gc.append(float(split[1]))
    rest.append(float(split[2]))
    std.append(float(split[3]))

gcLoad = []
for i in range(0, 5):
    gcLoad.append(load[i] + gc[i])

loadBar = ax.bar(ind, load, width, color='#414FAF', hatch='/')
gcBar = ax.bar(ind, gc, width, color='#FF6833', bottom=load, hatch='-')
restBar = ax.bar(ind, rest, width, color='#8BC34A', bottom=gcLoad, hatch='X')

# add some text for labels, title and axes ticks
ax.set_ylabel('Time (seconds)')
ax.set_title('Time to Process Updates')
ax.set_xticks(ind+width)
ax.set_xticklabels( ('Initial Run', 'Update 1', 'Update 10', 'Update 100', 'update 1000') )

ax.legend( (loadBar[0], gcBar[0], restBar[0]), ('Load', 'GC', 'Execution') )

def autolabel(rects):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x()+rect.get_width()/2., 1.05*height, '%d'%int(height),
                ha='center', va='bottom')

#autolabel(loadBar)
#autolabel(gcBar)

#plt.show()
plt.savefig(graph_dir + 'updates2.png')
