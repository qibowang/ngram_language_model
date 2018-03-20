package cn.edu.blcu.nlp.probJoinKatz;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text,Text,Text,Text>{
	
	private Text resValue = new Text();
	private String prob="";
	private String back="";
	private String joinValue;
	private int  itemsLen;
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		prob="";
		back="";
		for(Text value:values){
			joinValue = value.toString();
			itemsLen=joinValue.split("\t").length;
			if(itemsLen==1){
				back=joinValue;
			}else if(itemsLen==2){
				prob=joinValue;
			}
		}
		if(prob.length()!=0){
			if(back.length()!=0){
				resValue.set(prob+"\t"+back);
			}else{
				resValue.set(prob);
			}
			context.write(key, resValue);
		}
	}
}
