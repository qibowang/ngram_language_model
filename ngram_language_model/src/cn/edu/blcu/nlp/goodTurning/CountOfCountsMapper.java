package cn.edu.blcu.nlp.goodTurning;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountOfCountsMapper extends Mapper<Text, LongWritable, Text, IntWritable> {
	private Text resKey = new Text();
	private final IntWritable ONE = new IntWritable(1);

	private int wordsNum;
	private int startOrder = 1;
	private int endOrder = 3;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		startOrder = conf.getInt("startOrder", startOrder);
		endOrder = conf.getInt("endOrder", endOrder);
	}

	@Override
	protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
		wordsNum = key.toString().length();
		if (wordsNum >= startOrder && wordsNum <= endOrder) {
			resKey.set(wordsNum + "\t" + value.get());
			context.write(resKey, ONE);
		}
	}
}
