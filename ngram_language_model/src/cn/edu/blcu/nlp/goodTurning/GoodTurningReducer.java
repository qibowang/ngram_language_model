package cn.edu.blcu.nlp.goodTurning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class GoodTurningReducer extends Reducer<Text,Text,Text,Text>{
	
	//Logger log = LoggerFactory.getLogger(GoodTurningReducer.class);
	
	private String ngram;
	private String valueStr;
	private long ngramRawcount;
	private double ngramGTCount;
	private String items[];
	private int gtmin=1;
	private Text resKey = new Text();
	private Text resValue = new Text();
	private double gtProb;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		gtmin=conf.getInt("gtmin", gtmin);
	}
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		List<Text> list = new ArrayList<Text>();
		//resValue.set(ngramStr+"\t"+ngramGtCountD+"\t"+ngramRawCountL);
		long rawcountSum=0l;
		for(Text value:values){
			valueStr=value.toString();
			//log.info("value into reduce--->"+valueStr);
			items=valueStr.split("\t");
			rawcountSum+=Long.parseLong(items[2]);
			list.add(WritableUtils.clone(value, conf));
		}
		
		for(Text value:list){
			
			valueStr=value.toString();
			items=valueStr.split("\t");
			ngram=items[0];
			ngramRawcount=Long.parseLong(items[2]);
			ngramGTCount=Double.parseDouble(items[1]);
			if(ngramRawcount>=gtmin){
				resKey.set(ngram);
				gtProb=(double)ngramGTCount/rawcountSum;
				if(gtProb>1.0)
					gtProb=1.0;
				resValue.set(gtProb+"\t"+ngramRawcount);
				context.write(resKey, resValue);
			}
		}
		
	}
}
