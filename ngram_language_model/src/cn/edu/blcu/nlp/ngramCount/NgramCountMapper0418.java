package cn.edu.blcu.nlp.ngramCount;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
/*
 * 该Mapper是对Good-Turning修正之后重新开始的程序
 * */


public class NgramCountMapper0418 extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text resKey = new Text();
	private final IntWritable ONE = new IntWritable(1);
	private String ngram ="";
	private int startOrder=1;
	private int endOrdert=3;
	private String line;
	private int lineLen;
	private int index;
	private int orderTemp;
	private String items[];
	private String needSuppStr;
	private String preLine="";
	
	private final char SEP_CHAR='▲';
	private final char NUM_CHAR='■';
	//private final char CIRCULAR_CHAR='●';
	
	private final String SEP_STRING="▲";
	private final String NUM_STRING="■";
	//private final String CIRCULAR_STRING="●";
	
	private List<String> list;
	//Logger log = LoggerFactory.getLogger(NgramCountMapper.class);
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		startOrder = conf.getInt("startOrder", startOrder);
		endOrdert = conf.getInt("endOrder", endOrdert);
		list = new ArrayList<String>();
	}
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			line = new String(value.getBytes(), 0, value.getLength(), "gbk");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
		line=processLine(line);
		lineLen=line.length();
		line=preLine+line;
		//log.info("before replace--->"+line);
		line = line.replaceAll("▲+", SEP_STRING);
		line = line.replaceAll("■+", NUM_STRING);
		//line = line.replaceAll("●+", CIRCULAR_STRING);
		//log.info("after replace---->"+line);
		lineLen=line.length();
		
		if(lineLen<endOrdert){
			preLine=line;
		}else{
			for(String str:list){
				items=str.split("\t");
				needSuppStr=items[0];
				orderTemp=Integer.parseInt(items[1]);
				//log.info("before supp--->"+str);
				int tempIndex=0;//为了避免上一行的行尾和该行的行首都是标点
				boolean tempBool1=needSuppStr.charAt(orderTemp-2)==SEP_CHAR&&line.charAt(0)==SEP_CHAR;
				boolean tempBool2=needSuppStr.charAt(orderTemp-2)==NUM_CHAR&&line.charAt(0)==NUM_CHAR;
				//boolean tempBool3=needSuppStr.charAt(orderTemp-2)==CIRCULAR_CHAR&&line.charAt(0)==CIRCULAR_CHAR;
				//if(tempBool1||tempBool2||tempBool3)
				if(tempBool1||tempBool2)
					tempIndex=1;
				for(index=0;index<orderTemp-1;index++){
					ngram=needSuppStr.substring(index)+line.substring(tempIndex,index+tempIndex+1);
					//log.info("after supp--->"+ngram);
					resKey.set(ngram);
					context.write(resKey, ONE);
				}
				
			}
			list.clear();
			for(orderTemp=startOrder;orderTemp<=endOrdert;orderTemp++){
				for(index=0;index<=lineLen-orderTemp;index++){
					ngram = line.substring(index, index+orderTemp);
					//log.info("ngram--->"+ngram);
					resKey.set(ngram);
					context.write(resKey, ONE);
				}
				if(orderTemp>1){
					list.add(line.substring(index)+"\t"+orderTemp);
				}
			}
			preLine="";
			
		}
	}
	
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		list.clear();
	}
	private String processLine(String line) {
		String posPattern = "( )?/[a-zA-Z]{1,5}( )?";
		String numberRegrex = "\\d+[.,]?\\d*";
		
		line = line.replaceAll(posPattern, "");
		line = line.replaceAll(numberRegrex, "■");
		line = noneHZRep(line);
		line = line.replaceAll("(▲( ▲)*)+", SEP_STRING);
		line = line.replaceAll("(■( ■)*)+", NUM_STRING);
		//line = line.replaceAll("(●( ●)*)+", CIRCULAR_STRING);
		return line;
	}

	private static String noneHZRep(String line) {
		StringBuffer sb = new StringBuffer();
		char numSign = '■';
		char triangleSign = '▲';
		//char circularSign='●';
		
		
		char[] cArr = line.toCharArray();
		for (char ch : cArr) {
			if (ch >= '\u4e00' && ch <= '\u9fff')
				sb.append(ch);
			else if (ch == numSign)
				sb.append(ch);
			else if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z'))
				sb.append(ch);
			else if (ch == ' ')
				sb.append(' ');
			else
				sb.append(triangleSign);
			//else if (ch == ','||ch=='.'||ch=='?'||ch=='!'||ch==';'||ch==':'||ch=='，'||ch=='。'||ch=='！'||ch=='？'||ch=='；'||ch=='：')
			//	sb.append(triangleSign);
			//else
			//	sb.append(circularSign);
		}
		return sb.toString();
	}
	
}
