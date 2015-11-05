# HadoopMapReduce
It's representing the algorithms such as Pair, Stripes and Hybrid approaches in Map Reduce.


Create the HDFS file system
	Hadoop fs -put file.txt file.txt
	
View the HDFS file system (Check whether our file exist or not)
	Hadoop fs -ls
	
Run the project by using created HDFS
	Hadoop jar wordcount.jar WordCount file.txt wordcountoutput
