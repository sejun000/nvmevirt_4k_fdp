#!/bin/bash

rm -rf CSDVirt.o libCSDVirt.so
make
sudo cp libCSDVirtC.so /usr/lib/
