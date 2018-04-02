#!/bin/bash
jarfile='./target/proteus-solma_2.11-0.1.3-jar-with-dependencies.jar'

scp $jarfile zhanwang@ibm-power-1.dima.tu-berlin.de:/share/flink/onlineML
