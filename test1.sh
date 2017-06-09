#!/bin/bash

mvn clean package
cp target/accumulo-yield-test-0.0.1-SNAPSHOT.jar $ACCUMULO_HOME/lib/ext

accumulo shell -u root -p secret << EOC
config -s tserver.server.threads.minimum=128
config -s tserver.readahead.concurrent.max=128
config -s tserver.scan.files.open.max=128
EOC

accumulo-cluster restart

cat > test.properties << EOP
table=yieldTest
rows=5000000

suffix=973973
numLongScans=127
yieldCount=1000000

numShortScans=250
rangesPerScan=5
EOP

accumulo ayt.Write test.properties
accumulo ayt.LongScans test.properties &
sleep 1
accumulo ayt.RandomScans test.properties

wait
