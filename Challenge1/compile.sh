#! /bin/bash
mkdir -p build
javac -cp $(hadoop classpath) -d build src/*
jar cvf ${1}.jar -C build .
exit
