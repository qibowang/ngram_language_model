package cn.edu.blcu.nlp.goodTurning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import org.slf4j.LoggerFactory;

public class GoodTurningReducer extends Reducer<Text, Text, Text, Text> {

	// Logger log = LoggerFactory.getLogger(GoodTurningReducer.class);

	private String ngram;
	private int HZNum;
	private String HZNumStr;
	private String valueStr;
	private long ngramRawcountL;
	private double ngramGTCountD;
	private String items[];
	private int gtmin = 1;
	private Text resKey = new Text();
	private Text resValue = new Text();

	private double prob;

	private double discountRate;
	private double rateTemp;
	private final int COUNTBOUNDARY = 7;// ngramRawCount小于该值时进行
										// good-turning值对概率进行计算 否则 用原始次数对概率进行计算

	private long NrBoundary = 0;// ngram 中词频为boundary的总共有NrBoundary个
	private long Nr1;// ngram 中词频为1的总共有Nr1个
	private HashMap<String, Long> temp;
	private HashMap<String, HashMap<String, Long>> map = new HashMap<String, HashMap<String, Long>>();
	Logger log = LoggerFactory.getLogger(GoodTurningReducer.class);
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		gtmin = conf.getInt("gtmin", gtmin);
		String cocPath = conf.get("cocPath");
		Path path = new Path(cocPath + "/part-r-00000");

		Option option = Reader.file(path);
		Reader reader = new Reader(conf, option);
		Text key = new Text();
		LongWritable value = new LongWritable();
		String[] items;
		String HZNumStr;
		String ngramRawCountStr;
		long ngramRawCountL = 0l;
		Long frequenceL;

		while (reader.next(key, value)) {
			// key--->wordsNum+"\t"+ngramCount
			// value--->frequence
			items = key.toString().split("\t");
			HZNumStr = items[0];
			ngramRawCountStr = items[1];
			ngramRawCountL = Long.parseLong(ngramRawCountStr);
			frequenceL = value.get();
			if ((ngramRawCountL > COUNTBOUNDARY && ngramRawCountL <= COUNTBOUNDARY + 5) || ngramRawCountL == 1) {
				// if(ngramRawCountStr.equals(String.valueOf(COUNTBOUNDARY))||ngramRawCountStr.equals("1")){
				if (map.containsKey(HZNumStr)) {
					temp = map.get(HZNumStr);
					temp.put(ngramRawCountStr, frequenceL);
					map.put(HZNumStr, temp);
				} else {
					temp = new HashMap<String, Long>();
					temp.put(ngramRawCountStr, frequenceL);
					map.put(HZNumStr, temp);
				}
			}
		}
		IOUtils.closeStream(reader);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		List<Text> list = new ArrayList<Text>();
		
		/*Iterator iter = map.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry)iter.next();
			String key1 = (String) entry.getKey();
			log.info("HZ NUM--->"+key1);
			Map<String,Long> innerMap = (Map<String, Long>) entry.getValue();
			Iterator innerIter = innerMap.entrySet().iterator();
			while(innerIter.hasNext()){
				Map.Entry innerEntery = (Map.Entry)innerIter.next();
				String innerKey = (String) innerEntery.getKey();
				long count = (long) innerEntery.getValue();
				log.info("innerKey--->"+innerKey);
				log.info("innerValue--->"+count);
			}
			
		}*/
		//resValue.set(ngramStr+"\t"+ngramGtCountD+"\t"+ngramRawCountL);
		long rawcountSum = 0l;
		for (Text value : values) {
			valueStr = value.toString();
			// log.info("value into reduce--->"+valueStr);
			items = valueStr.split("\t");
			rawcountSum += Long.parseLong(items[2]);
			list.add(WritableUtils.clone(value, conf));
			
		}

		for (Text value : list) {
			valueStr = value.toString();
			items = valueStr.split("\t");
			ngram = items[0];
			HZNum = ngram.length();
			HZNumStr = String.valueOf(HZNum);
			//log.info("ngram--->"+ngram);
			//log.info("HZNum--->"+HZNum);
			ngramRawcountL = Long.parseLong(items[2]);
			ngramGTCountD = Double.parseDouble(items[1]);
			prob = (double) ngramRawcountL / rawcountSum;
			if (ngramRawcountL >= gtmin) {
				resKey.set(ngram);
				if(HZNum==1){
					resValue.set(prob + "\t" + ngramRawcountL);
					context.write(resKey, resValue);
				}else{
					temp = map.get(HZNumStr);
					for (int i = 1; i <= 5; i++) {
						if (temp.containsKey(String.valueOf(COUNTBOUNDARY + i))) {
							NrBoundary = temp.get(String.valueOf(COUNTBOUNDARY + i));
							break;
						}
					}
					Nr1 = temp.get("1");
					rateTemp = (double) (COUNTBOUNDARY + 1) * NrBoundary / Nr1;// A
					discountRate = ((double) ngramGTCountD / ngramRawcountL - rateTemp) / (1 - rateTemp);
					if (ngramRawcountL <= COUNTBOUNDARY) {
						prob *= discountRate;
					}
					if (prob > 1.0)
						prob = 1.0;
					resValue.set(prob + "\t" + ngramRawcountL);
					context.write(resKey, resValue);
				}	
			}
		}

	}
}
