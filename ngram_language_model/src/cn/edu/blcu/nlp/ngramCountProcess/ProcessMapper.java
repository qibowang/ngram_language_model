package cn.edu.blcu.nlp.ngramCountProcess;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProcessMapper extends Mapper<Text, LongWritable, Text, LongWritable> {
	private Text resKey = new Text();
	private String ngram = "";
	private int wordsNum;
	private int startOrder = 1;
	private int endOrder = 5;
	private final String SEP_STRING = "▲";
	private final String CIRCULAR_STRING = "●";

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		startOrder = conf.getInt("startOrder", startOrder);
		endOrder = conf.getInt("endOrder", endOrder);
	}

	@Override
	protected void map(Text key, LongWritable value, Mapper<Text, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		ngram = key.toString();
		wordsNum = ngram.length();
		if (wordsNum >= startOrder && wordsNum <= endOrder) {
			ngram = ngram.replaceAll(CIRCULAR_STRING, SEP_STRING);
			resKey.set(ngram);
			context.write(resKey, value);
		}
	}
}
