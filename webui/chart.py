import matplotlib.pyplot as plt
import sys

webui_root = "webui/"

points = []
for i in range(1, len(sys.argv)):
    points.append(sys.argv[i])

plt.plot(points)
plt.ylabel('some numbers')
plt.savefig(webui_root + "tasks.png")
