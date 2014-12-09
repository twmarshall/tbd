#!/usr/bin/env python
# encoding: utf-8

import sys
import os

def main():
    f = open('wiki1.xml','w')
    for i in range(1,1001):
        f.write("<key>")
        f.write(str(i))
        f.write("</key><value>")
        f.write(str(1001-i))
        f.write('</value>\n')
    f.close()

    f = open('wiki2.xml','w')
    for i in range(1001,2001):
        f.write("<key>")
        f.write(str(i))
        f.write("</key><value>")
        f.write(str(3001-i))
        f.write('</value>\n')
    f.close()

    f = open('wiki3.xml','w')
    for i in range(2001,3001):
        f.write("<key>")
        f.write(str(i))
        f.write("</key><value>")
        f.write(str(5001-i))
        f.write('</value>\n')
    f.close()

if __name__ == '__main__':
    main()

