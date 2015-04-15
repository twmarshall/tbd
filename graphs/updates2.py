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
total = []
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

    load.append(float(split[1]) / 1000)
    gc.append(float(split[2]) / 1000)
    rest.append(float(split[3]) / 1000)
    total.append(float(split[4]) / 1000)
    std.append(float(split[5]) / 1000)

width = .5

x = np.arange(len(load)) + .5  # the x locations for the groups

fig, ax = plt.subplots()

gcLoad = []
for i in range(0, len(load)):
    gcLoad.append(load[i] + gc[i])

loadBar = ax.bar(x, load, width, color='#414FAF', hatch='/')
gcBar = ax.bar(x, gc, width, color='#FF6833', bottom=load, hatch='-')
restBar = ax.bar(x, rest, width, color='#8BC34A', bottom=gcLoad, hatch='X', yerr=std)

ax.set_xticks(x + width/2)
ax.set_xticklabels(labels)

ax.set_ylabel('Time (seconds)')
ax.set_ylim(bottom = 0, top = 15)
ax.set_xlim(0, 2.5)
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
