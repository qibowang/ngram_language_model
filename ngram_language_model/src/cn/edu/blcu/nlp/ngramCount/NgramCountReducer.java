package cn.edu.blcu.nlp.ngramCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by root on 2017/5/24.
 */
public class NgramCountReducer extends Reducer<Text,IntWritable,Text,LongWritable> {
	private LongWritable resValue = new LongWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		long sum=0l;
		for(IntWritable i:values){
			sum+=i.get();
		}
		resValue.set(sum);
		context.write(key,resValue);
	}
}
