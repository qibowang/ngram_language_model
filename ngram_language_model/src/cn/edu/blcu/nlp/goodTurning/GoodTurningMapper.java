package cn.edu.blcu.nlp.goodTurning;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GoodTurningMapper extends Mapper<Text, LongWritable, Text, Text> {
	// map 外层key 是ngram中n 1 2 3 4 5
	// 内层是一个hash 其中key是原始ngram频次 value是出现key次的总共有多少个
	private HashMap<String, HashMap<String, Long>> map = new HashMap<String, HashMap<String, Long>>();
	private Text resKey = new Text();
	private Text resValue = new Text();

	private String ngramStr;

	private long ngramRawCountL;
	private String ngramRawCountStr;
	private double ngramGtCountD;
	private int HZNum;
	private String HZNumStr;

	private int startOrder = 1;
	private int endOrder = 3;

	private int tempI;
	private String prefix = "";
	private int gtBoundary=5;//ngramRawCount小于该值时进行 good-turning平滑 该值的设置参考宗成庆老师书给的建议值

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		startOrder = conf.getInt("startOrder", startOrder);
		endOrder = conf.getInt("endOrder", endOrder);

		String cocPath = conf.get("cocPath");
		Path path = new Path(cocPath + "/part-r-00000");

		Option option = Reader.file(path);
		Reader reader = new Reader(conf, option);
		Text key = new Text();
		LongWritable value = new LongWritable();
		String[] items;
		String HZNumStr;
		String ngramRawCountStr;
		Long frequenceL;
		HashMap<String, Long> temp;
		while (reader.next(key, value)) {
			// key--->wordsNum+"\t"+ngramCount
			// value--->frequence
			items = key.toString().split("\t");
			HZNumStr = items[0];
			ngramRawCountStr = items[1];
			frequenceL = value.get();
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
		IOUtils.closeStream(reader);
	}

	@Override
	protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
		ngramStr = key.toString();
		ngramRawCountL = value.get();
		ngramGtCountD = (double) ngramRawCountL;
		ngramRawCountStr = String.valueOf(ngramRawCountL);
		HZNum = ngramStr.length();
		HZNumStr = String.valueOf(HZNum);

		if (HZNum >= startOrder && HZNum <= endOrder) {
			if (ngramRawCountL <= gtBoundary) {
				for (tempI = 1; tempI < 5; tempI++) {
					if (map.get(HZNumStr).containsKey(ngramRawCountStr)
							&& map.get(HZNumStr).containsKey(String.valueOf(ngramRawCountL + tempI))) {
						Long Nr1 = map.get(HZNumStr).get(String.valueOf(ngramRawCountL + tempI));
						Long Nr = map.get(HZNumStr).get(ngramRawCountStr);
						ngramGtCountD = (ngramRawCountL + 1d) * Nr1.doubleValue() / Nr.doubleValue();
						break;
					}
				}

				resValue.set(ngramStr + "\t" + ngramGtCountD + "\t" + ngramRawCountL);

				if (HZNum == 1) {
					resKey.set("unigram");
					context.write(resKey, resValue);
				} else {
					prefix = ngramStr.substring(0, HZNum - 1);
					resKey.set(prefix);
					context.write(resKey, resValue);
				}

			}
		} else {

			resValue.set(ngramStr + "\t" + ngramRawCountL + "\t" + ngramRawCountL);
			if (HZNum == 1) {
				resKey.set("unigram");
				context.write(resKey, resValue);
			} else {
				prefix = ngramStr.substring(0, HZNum - 1);
				resKey.set(prefix);
				context.write(resKey, resValue);
			}

		}

	}

}
