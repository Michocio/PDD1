# plik hadoopPath w pierwszej lini powinien zawierac sciezke do instalacji hadoopa
# skrypt instalujacy, ktory dostarczam tworzy ten plik i wpisuje sciezke,
# w przypadku innego sposobu instalacji nalezy utworzyc ten plik recznie.
pathToHadoop=hadoop-2.8.3
pathToCode=src/

echo "$pathToHadoop/bin/yarn classpath"

# Przenies przykladowy test na hdfs
#$pathToHadoop/bin/hdfs dfs -mkdir /user/mj348711/input
#$pathToHadoop/bin/hdfs dfs -put test.in /user/mj348711/input

export CLASSPATH=`$pathToHadoop/bin/yarn classpath`
javac -classpath gson-2.3.1.jar:/$CLASSPATH -d . $pathToCode/*.java
jar cf shl.jar *.class
rm *.class

rand=`date +%N`
#$pathToHadoop/bin/yarn jar shl.jar SimilarityJoin /user/mj348711/grupaA /user/mj348711/grupaB /user/mj348711/out_$rand /user/mj348711/coeff.in
$pathToHadoop/bin/yarn jar shl.jar SimilarityJoin $1 $2 /user/mj348711/out_$rand /user/mj348711/coeff.in
