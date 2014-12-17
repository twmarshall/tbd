#!/bin/bash

i=0
while [ $? == 0 ] && [ $i -lt 10 ]
do
  i=$(($i + 1))
  sbt/sbt test
done