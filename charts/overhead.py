#!/usr/bin/env python
# a bar plot with errorbars
import matplotlib.pyplot as plt
import numpy as np
import string

graph_dir = "charts/"
N = 3
width = 0.4       # the width of the bars
ind = np.arange(N)  # the x locations for the groups
fig, ax = plt.subplots()

means = [30, 21, 18]
rects1 = ax.bar(ind, means, width, color='#3c78d8')

# add some text for labels, title and axes ticks
ax.set_xlabel('Chunk Size')
ax.set_ylabel('Overhead')
ax.set_title('Storage Overhead')
ax.set_xticks(ind+width / 2)
ax.set_xticklabels( ('1', '10', '100') )
ax.set_xlim([-width, (N - 1) + 2 * width])
ax.set_ylim([0, 32])

#ax.legend( (rects1[0]), ('TDB', 'Non-incremental', 'Update 10') )

def autolabel(rects):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x()+rect.get_width()/2., height + .1, '%d'%int(height),
                ha='center', va='bottom')

autolabel(rects1)
#autolabel(rects2)

#plt.show()
plt.savefig(graph_dir + 'overhead.png')
