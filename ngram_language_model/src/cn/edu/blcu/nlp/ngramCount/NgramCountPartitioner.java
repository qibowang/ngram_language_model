package cn.edu.blcu.nlp.ngramCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Created by root on 2017/5/24.
 */
public class NgramCountPartitioner extends HashPartitioner<Text,IntWritable>{
	String keyStr="";
	String prefix="";
	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		keyStr=key.toString();
		prefix =(keyStr.length()>1)?(keyStr.substring(0, 2)):keyStr;
		return Math.abs(prefix.hashCode())%numReduceTasks;
	}
}
