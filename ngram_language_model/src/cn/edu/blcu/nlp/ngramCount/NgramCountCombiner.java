package cn.edu.blcu.nlp.ngramCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by root on 2017/5/24.
 */
public class NgramCountCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{
	private IntWritable resValue = new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum=0;
		for(IntWritable i:values){
			sum+=i.get();
		}
		resValue.set(sum);
		context.write(key,resValue);
	}
}
