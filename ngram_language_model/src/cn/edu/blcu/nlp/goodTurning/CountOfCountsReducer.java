package cn.edu.blcu.nlp.goodTurning;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class CountOfCountsReducer extends Reducer<Text,IntWritable,Text,LongWritable>{
	private LongWritable resValue = new LongWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		long sum=0l;
		for(IntWritable value:values){
			sum+=value.get();
		}
		resValue.set(sum);
		context.write(key, resValue);
	}
}
