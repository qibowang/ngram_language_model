package cn.edu.blcu.nlp.katz;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.hadoop.compression.lzo.LzoCodec;


/*
 * 将后缀串和对应概率
 * */
public class GtProbJoinSuffixProb {
	public static class KatzDenominatorMapper extends Mapper<Text, Text, Text, Text> {
		private Text resKey = new Text();
		private Text resValue = new Text();
		private String ngram;
		private int wordsNum;
		private String suffix;
		private String valueStr;
		private String items[];
		
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			// key:ngram
			// value:prob ngramCount
			ngram = key.toString();
			wordsNum = ngram.length();
			valueStr=value.toString();
			
			if (wordsNum >= 2) {
				suffix = ngram.substring(1);
				items=valueStr.split("\t");
				resKey.set(suffix);
				resValue.set(ngram+"\t"+items[0]);
				context.write(resKey, resValue);
				//
				resValue.set(items[0]);
				context.write(key, resValue);
			}
			
		}
	}

	public static class KatzDenominatorReducer extends Reducer<Text, Text, Text, Text> {
		private String items[];
		private Text resKey = new Text();
		private Text resValue = new Text();
		private String probStr = "";
		private int listSize = 0;
		private int tempIndex = 0;
		private String tempStr;

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			List<Text> list = new ArrayList<Text>();

			for (Text value : values) {
				items = value.toString().split("\t");
				if (items.length == 1) {
					// value:prob
					probStr = items[0];
				} else {
					list.add(WritableUtils.clone(value, conf));
				}
			}
			listSize = list.size();
			if (probStr.length() != 0 && listSize != 0) {
				for (tempIndex = 0; tempIndex < listSize; tempIndex++) {
					tempStr=list.get(tempIndex).toString();
					//ngram\\tprob\\tngramCount
					items=tempStr.split("\t");
					resKey.set(items[0]);
					resValue.set(items[1]+"\t"+probStr);
					context.write(resKey, resValue);
				}
			}
		}
	}
	
	public static void main(String[] args) {
		String input="";
		String output="";
		int isLzo=0;
		int tasks = 1;
		
		
		boolean parameterValid=false;
		int parameterNum = args.length;
		String inputPaths[]=new String[10];
		int index=0;

		for (int i = 0; i < parameterNum; i++) {
			if (args[i].startsWith("-input")) {
				
				input = args[++i];
				if(index<inputPaths.length){
					inputPaths[index++]=input;
				}else{
					System.out.println("input paths are more than 10 please build the jar file again");
					parameterValid=true;
				}
				System.out.println("input path--->"+input);
			} else if (args[i].equals("-output")) {
				output = args[++i];
				System.out.println("output--->" + output);
			}  else if (args[i].equals("-isLzo")) {
				isLzo = Integer.parseInt(args[++i]);
				System.out.println("isLzo---->" + isLzo);
			} else if(args[i].equals("-tasks")){
				tasks=Integer.parseInt(args[++i]);
				System.out.println("tasks--->"+tasks);
			}else {
				System.out.println("there exists invalid parameters--->" + args[i]);
				parameterValid=true;
			}
		}

		if(parameterValid){
			System.out.println("parameters invalid!!!!");
			System.exit(1);
		}
		
		try {

			Configuration conf = new Configuration();
		
			conf.setBoolean("mapreduce.compress.map.output", true);
			conf.setClass("mapreduce.map.output.compression.codec", LzoCodec.class, CompressionCodec.class);
			conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
			conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
			
			Job probJoinSuffix = Job.getInstance(conf, "gtProb Join suffix");
			System.out.println(probJoinSuffix.getJobName() + " is running!!!");
			probJoinSuffix.setJarByClass(GtProbJoinSuffixProb.class);

			probJoinSuffix.setMapperClass(KatzDenominatorMapper.class);
			probJoinSuffix.setReducerClass(KatzDenominatorReducer.class);
		
			
			probJoinSuffix.setMapOutputKeyClass(Text.class);
			probJoinSuffix.setMapOutputValueClass(Text.class);
			probJoinSuffix.setOutputKeyClass(Text.class);
			probJoinSuffix.setOutputValueClass(Text.class);
			probJoinSuffix.setNumReduceTasks(tasks);
			
			
			for(String path:inputPaths){
				if(path!=null){
					System.out.println("input path--->"+path);
					FileInputFormat.addInputPath(probJoinSuffix, new Path(path));
				}
			}
			
			FileInputFormat.setInputDirRecursive(probJoinSuffix, true);
			FileSystem fs = FileSystem.get(conf);
			Path outputPath = new Path(output);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(probJoinSuffix, outputPath);
			
			probJoinSuffix.setOutputFormatClass(SequenceFileOutputFormat.class);
			if (isLzo == 0) {
				setLzo(probJoinSuffix);
			}

			if (probJoinSuffix.waitForCompletion(true)) {
				System.out.println(probJoinSuffix.getJobName() + " successed");
			} else {
				System.out.println(probJoinSuffix.getJobName() + " failed");
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		
	}
	public static void setLzo(Job job) {
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
	}
}
