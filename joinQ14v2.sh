#! /bin/sh
# Chunwei Liu
# RLE BIT_PACKED 
BASEDIR=$(pwd)
for i in 1 2; do
        cd ~/tpch-generator/dbgen/
        ./dbgen -f -s $i
        echo "scale:$i"
        for p_key in DICT; do
                for l_key in DICT; do
                        echo "${p_key},${l_key} part.0 lineitem.1"
                        cd ~/my_encsel/
                        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.GlobalJoinFileProducer $p_key $l_key UNCOMPRESSED
                        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.HashJoinTool
                        #cd $BASEDIR
                done
        done
done
echo "query ended!"
