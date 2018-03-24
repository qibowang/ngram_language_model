package cn.edu.blcu.nlp.probJoinKatz;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProbMapper extends Mapper<Text, Text, Text, Text> {

	private String valueStr;
	private String items[];
	private String ngramCountString = "";
	private String probStr = "";
	private Text resValue = new Text();
	private int gtmin = 3;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		gtmin = conf.getInt("gtmin", gtmin);
	}

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		valueStr = value.toString();
		items = valueStr.split("\t");

		if (items.length == 2) {
			probStr = items[0];
			ngramCountString = items[1];
			if (Long.parseLong(ngramCountString) >= gtmin) {
				resValue.set(Math.log10(Double.parseDouble(probStr)) + "\t" + ngramCountString);
				context.write(key, resValue);
			}
		}
	}

}
