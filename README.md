TBD
===========

Build with SBT (you don't need to install SBT on your machine)

# sbt/sbt

> compile

> test

> mkrun

The 'mkrun' command will create the files bin/master.sh, which can be used to launch a master, and bin/experiment, which runs the experiments.

To turn on debug log output, edit core/src/main/resources/common.conf, changing
loglevel to "DEBUG".