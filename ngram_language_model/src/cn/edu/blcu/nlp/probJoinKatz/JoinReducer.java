package cn.edu.blcu.nlp.probJoinKatz;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text,Text,Text,Text>{
	private Text resKey = new Text();
	private Text resValue = new Text();
	private String prob="";
	private String back="";
	private String joinValue;
	private int  itemsLen;
	private String ngram;
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		ngram = key.toString();
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
			resKey.set(new StringBuffer(ngram).reverse().toString());
			context.write(resKey, resValue);
		}
	}
}
