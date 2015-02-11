#!/bin/sh
rm -rf jars/*
git pull -u
mvn -e -X clean dependency:copy-dependencies package
cp `find target/dependency/*.jar` jars
cp target/SimpleSpark-0.0.1-SNAPSHOT.jar jars

