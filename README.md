TBD
===========

Build with SBT (you don't need to install SBT on your machine)

```
> sbt/sbt
> compile
> test
```

## Experiment
```
>sbt/sbt
> mkrun
```

This will create the script bin/experiment.sh which can be used to run the
performance experiments.

To run in cluster mode, 'mkrun' will also create the script bin/master.sh
which can be used to launch a master. This script will print out a URL of
the form 'akka.tcp://...' which can then be passed to the bin/worker.sh
script as a required trailing argument. Then, you can run bin/experiment.sh with
the '--master' flag with this URL.

## Debugging

To turn on debugging output from the library, you can change the imports at 
the top of any source files containing self-adjusting code from:

```
import tbd._
import tbd.TBD._
```

to

```
import tbd.debug._
import tbd.debug.TBD._
```

This is required to make the visualizer work, for example modifying ModList.scala and
ModListNode.scala.

## Visualizing

The visualizer can be used to visualize traces and dependencies. Furthermore, the visualizer can be used to calculate intrinsic trace distance. Make sure to turn on debugging, as described above, to use the visualizer.

```
> sbt/sbt
> mkvisualization
```

Run examples:

```
> bin/visualization.sh --help
> bin/visualization.sh -a sort
> bin/visualization.sh -a map -o diff
> bin/visualization.sh -a map -o diff -t manual
> bin/visualization.sh -a sort -o plot2d -t exhaustive
> bin/visualization.sh -a reduce -i 5 -o chart2d -t exhaustive
```

To call a quick visualizer from within your code (for debugging), use: 

```
tbd.visualization.QuickVisualizer.show(mutator.getDDG())
```

*Visualizer UI*

Select mutation round from combo box.
Click node for details in lower panel
Black edges: Control dependency.
Red edges: Mod dependencies (mod <-> write or read <-> write)
Dashed edges: Bound free vars, whereas color corresponds to color of description

If two different DDGs are selected in diff mode, the program will show intrinsic trace distance and distance accomplished by TBD change propagation in the lowermost panel.
Removed/Added nodes will be highlighted in yellow/red.

Scroll by dragging the mouse.
Zoom with the scroll wheel. 
