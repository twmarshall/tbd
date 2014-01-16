TBD
===========

Build with SBT (Note: we only support sbt 0.12.0 due to a bug in 0.13.0
outlined here: https://github.com/sbt/sbt/issues/810, but as we include the 
sbt jar in the repo, you shouldn't need to install anything anyways):

# sbt/sbt

> compile

> test

To turn on debug log output, edit core/src/main/resources/common.conf, changing
loglevel to "DEBUG".