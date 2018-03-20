package cn.edu.blcu.nlp.probJoinKatz;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class ProbMapper extends Mapper<LongWritable,Text,Text,Text>{
	private String ngram;
	private String valueStr;
	private String items[];
	private Text resKey = new Text();
	private Text resValue = new Text();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		valueStr=value.toString();
		items=valueStr.split("\t");
		if(items.length==3){
			resKey.set(items[0]);
			resValue.set(items[1]+"\t"+items[2]);
			context.write(resKey, resValue);
		}
				
	}
	
}
