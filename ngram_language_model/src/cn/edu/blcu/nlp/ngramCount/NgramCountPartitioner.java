package cn.edu.blcu.nlp.ngramCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Created by root on 2017/5/24.
 */
public class NgramCountPartitioner extends HashPartitioner<Text,IntWritable>{
	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		String line=key.toString();
		String prefix =(line.length()>1)?(line.substring(0, 2)):line.substring(0, 1);
		return Math.abs(prefix.hashCode())%numReduceTasks;
	}
}
