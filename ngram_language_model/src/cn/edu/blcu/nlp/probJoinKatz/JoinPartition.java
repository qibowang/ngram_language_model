package cn.edu.blcu.nlp.probJoinKatz;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class JoinPartition extends HashPartitioner<Text, Text>{
	String keyStr="";
	String prefix="";
	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
		keyStr=key.toString();
		prefix =(keyStr.length()>1)?(keyStr.substring(0, 2)):keyStr;
		return Math.abs(prefix.hashCode())%numReduceTasks;
	}
}
