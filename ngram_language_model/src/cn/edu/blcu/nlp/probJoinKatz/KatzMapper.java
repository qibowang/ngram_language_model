package cn.edu.blcu.nlp.probJoinKatz;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class KatzMapper extends Mapper<LongWritable,Text,Text,Text>{
	private Text resKey = new Text();
	private Text resValue = new Text();
	private String valueStr;
	private String items[];
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		valueStr=value.toString();
		items=valueStr.split("\t");
		if(items.length==2){
			resKey.set(items[0]);
			resValue.set(items[1]);
			context.write(resKey, resValue);
		}
	}
	
	
}
