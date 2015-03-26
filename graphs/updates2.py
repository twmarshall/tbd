#!/usr/bin/env python
import matplotlib
# prevents pyplot from trying to connect to x windowing
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import numpy as np
import string
import sys

graph_dir = "graphs/"

# Load results
file = open("results.txt", 'r')
labels = []
load = []
gc = []
rest = []
std = []
for line in file:
    split = string.split(line)

    label = split[0]
    if label == "initial":
        labels.append("Initial Run")
    elif label == "naive":
        labels.append("Non-incremental")
    else:
        labels.append("Update " + label)

    load.append(float(split[1]))
    gc.append(float(split[2]))
    rest.append(float(split[3]))
    std.append(float(split[4]))

width = 0.35       # the width of the bars
ind = np.arange(len(load))  # the x locations for the groups

fig, ax = plt.subplots()

gcLoad = []
for i in range(0, len(load)):
    gcLoad.append(load[i] + gc[i])

loadBar = ax.bar(ind, load, width, color='#414FAF', hatch='/')
gcBar = ax.bar(ind, gc, width, color='#FF6833', bottom=load, hatch='-')
restBar = ax.bar(ind, rest, width, color='#8BC34A', bottom=gcLoad, hatch='X', yerr=std)

# Labeling
ax.set_title('Time to Process Updates')

ax.set_xticks(ind + width/2)
ax.set_xticklabels(labels)

ax.set_ylabel('Time (seconds)')
ax.set_ylim(bottom = 0)
#ax.set_yscale("log")

ax.legend( (loadBar[0], gcBar[0], restBar[0]), ('Load', 'GC', 'Execution') )

def autolabel(rects):
    # attach some text labels
    for rect in rects:
        height = rect.get_height() + rect.get_y()
        ax.text(rect.get_x()+rect.get_width()/2., 1.01 * height, '%d'%int(height),
                ha='center', va='bottom')

autolabel(restBar)

if len(sys.argv) > 1:
    plt.savefig(graph_dir + sys.argv[1])
else:
    plt.savefig(graph_dir + "updates2.png")
