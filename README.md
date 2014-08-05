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
> mkexamples
```

This will create the script bin/experiment.sh which can be used to run the
performance experiments.

## Debuggins

To turn on debug log output, edit core/src/main/resources/common.conf, changing
loglevel to "DEBUG".

To turn on more useful DDG output, edit core/src/main/scala/tbd/master/Main.scala,
changing 'val debug' to 'true'.

## Visualizing

The visualizer can be used to visualize traces and dependencies. Furthermore, the visualizer can be used to calculate intrinsic trace distance.

```
> sbt/sbt
> mkvisualization
```

To run:

```
> bin/visualization.sh --help
> bin/visualization.sh -a quicksort
> bin/visualization.sh -a map -d
```

To call a quick visualizer from within your code (for debugging), use: 

```
tbd.visualization.QuickVisualizer.show(mutator.getDDG())
```

*Short explanation for visualizer UI*

Select mutation round from combo box.
Click node for details in lower panel
Black edges: Control dependency.
Red edges: Mod dependencies (mod <-> write or read <-> write)
Dashed edges: Bound free vars, whereas color corresponds to color of description

If two different DDGs are selected in diff mode, the program will show intrinsic trace distance and distance accomplished by TBD change propagation in the lowermost panel.
Removed/Added nodes will be highlighted in yellow/red.

Scroll by dragging the mouse.
Zoom with the scroll wheel. 
