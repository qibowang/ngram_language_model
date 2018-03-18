package cn.edu.blcu.nlp.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hadoop.compression.lzo.LzoCodec;

public class ProbSort {
	
	public static class ProbSortMapper extends Mapper<Text,Text,Text,Text>{
		private String ngram;
		private String valueStr;
		private String items[];
		private Text resKey = new Text();
		private Text resValue = new Text();
		
		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			ngram=key.toString();
			resKey.set(new StringBuffer(ngram).reverse().toString());
			valueStr=value.toString();
			items=valueStr.split("\t");
			resValue.set(String.valueOf(Math.log10(Double.parseDouble(items[0])))+"\t"+items[1]);
			context.write(resKey, resValue);
		}
	}
	
	public static class ProbSortReducer extends Reducer<Text,Text,Text,Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for(Text value:values){
				context.write(key, value);
			}
		}
	}
	
	public static class ProbSortComparator extends WritableComparator{
		protected ProbSortComparator(){
			super(Text.class,true);
		}
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Text text1 = (Text)a;
			Text text2 = (Text)b;
			String str1=text1.toString();
			String str2=text2.toString();
			return str1.compareTo(str2);
		}
	}
	
	public static void main(String[] args) {
		String input = "";
		String output = "";
		
		boolean parameterValid = false;
		int parameters = args.length;
		String inputPaths[]=new String[10];
		int index=0;
		for (int i = 0; i < parameters; i++) {
			if (args[i].equals("-input")) {
				input = args[++i];
				if(index<inputPaths.length){
					inputPaths[index++]=input;
				}else{
					System.out.println("input paths are more than 10 please build the jar file again");
				}
				System.out.println("input--->" + input);
				
			} else if (args[i].equals("-output")) {
				output = args[++i];
				System.out.println("output--->" + output);
			} else {
				System.out.println("there exists invalid parameters--->" + args[i]);
				parameterValid = true;
			}
		}
		if (parameterValid) {
			System.out.println("parameters invalid!!!!");
			System.exit(1);
		}
		try {

			Configuration conf = new Configuration();
			conf.setBoolean("mapreduce.compress.map.output", true);
			conf.setClass("mapreduce.map.output.compression.codec", LzoCodec.class, CompressionCodec.class);
			conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
			conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
			Job probSortJob = Job.getInstance(conf, "prob sort Job");
			System.out.println(probSortJob.getJobName() + " is running!");
			probSortJob.setJarByClass(ProbSort.class);
			probSortJob.setMapperClass(ProbSortMapper.class);
			probSortJob.setReducerClass(ProbSortReducer.class);
			probSortJob.setSortComparatorClass(ProbSortComparator.class);
			probSortJob.setNumReduceTasks(1);

			probSortJob.setInputFormatClass(SequenceFileInputFormat.class);
			probSortJob.setMapOutputKeyClass(Text.class);
			probSortJob.setMapOutputValueClass(Text.class);
			probSortJob.setOutputKeyClass(Text.class);
			probSortJob.setOutputValueClass(Text.class);

			for(String path:inputPaths){
				if(path!=null){
					System.out.println("input path--->"+path);
					FileInputFormat.addInputPath(probSortJob, new Path(path));
				}
			}
			
			FileInputFormat.setInputDirRecursive(probSortJob, true);
			FileSystem fs = FileSystem.get(conf);
			Path outputPath = new Path(output);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(probSortJob, outputPath);

			if (probSortJob.waitForCompletion(true)) {
				System.out.println(probSortJob.getJobName() + " Job successed");
			} else {
				System.out.println(probSortJob.getJobName() + " Job failed");
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}
}
