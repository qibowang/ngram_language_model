package cn.edu.blcu.nlp.ngramCountProcess;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProcessReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
	LongWritable resValue = new LongWritable();
	@Override
	protected void reduce(Text ngram, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
		long sum=0l;
		for(LongWritable value:values){
			sum+=value.get();
		}
		resValue.set(sum);
		context.write(ngram, resValue);
	}
}
