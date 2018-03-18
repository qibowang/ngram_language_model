package cn.edu.blcu.nlp.katz;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.hadoop.compression.lzo.LzoCodec;



public class Katz {
	public static class KatzMapper extends Mapper<Text, Text, Text, Text> {
		private Text resKey = new Text();

		private String ngram;
		private int HZNum;
		private String prefix;
		private int startOrder=1;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			startOrder=conf.getInt("startOrder", startOrder);
		}
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			ngram = key.toString();
			HZNum = ngram.length();
			if (HZNum >startOrder) {
				prefix = ngram.substring(0, HZNum - 1);
				resKey.set(prefix);
				context.write(resKey, value);
			}
		}

	}

	public static class KatzReducer extends Reducer<Text, Text, Text, Text> {
		private double back = 0.0;
		private double numerator = 0.0;
		private double denominator = 0.0;
		private String valueStr;
		private String[] items;
		private Text resValue = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				valueStr = value.toString();
				items = valueStr.split("\t");
				numerator += Double.parseDouble(items[0]);
				denominator += Double.parseDouble(items[1]);
			}
			if (numerator >= 1.0 || denominator >= 1.0) {
				back = 0.0;
			} else {
				back = Math.log10(1 - numerator) - Math.log10(1 - denominator);
			}
			
			resValue.set(String.valueOf(back));
			context.write(key, resValue);
		}
	}
	public class KatzSortComparator extends WritableComparator{
		protected KatzSortComparator() {
			super(Text.class,true);
		}
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			
			Text text1=(Text)a;
			Text text2=(Text)b;
			
			String str1=text1.toString();
			String str2=text2.toString();
			return str1.compareTo(str2);
			
		}
		
	}
	public static void main(String[] args) {
		String input = "";
		String backPath = "";
		int isLzo = 0;
		int tasks = 1;
		int startOrder=1;
		boolean parameterValid = false;
		int parameterNum = args.length;
		String inputPaths[] = new String[10];
		int index = 0;

		for (int i = 0; i < parameterNum; i++) {
			if (args[i].startsWith("-input")) {

				input = args[++i];
				if (index < inputPaths.length) {
					inputPaths[index++] = input;
				} else {
					System.out.println("input paths are more than 10 please build the jar file again");
					parameterValid = true;
				}
				System.out.println("input path--->" + input);
			} else if (args[i].equals("-output")) {
				backPath = args[++i];
				System.out.println("backPath--->" + backPath);
			} else if (args[i].equals("-isLzo")) {
				isLzo = Integer.parseInt(args[++i]);
				System.out.println("isLzo---->" + isLzo);
			} else if (args[i].equals("-tasks")) {
				tasks = Integer.parseInt(args[++i]);
				System.out.println("tasks--->" + tasks);
			} else if(args[i].equals("-startOrder")){
				startOrder=Integer.parseInt(args[++i]);
				System.out.println("startOrder--->"+startOrder);
			}else {
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
			conf.setInt("startOrder", startOrder);
			
			Job katzJob = Job.getInstance(conf, "gtProb Join suffix");
			System.out.println(katzJob.getJobName() + " is running!!!");
			katzJob.setJarByClass(GtProbJoinSuffixProb.class);

			katzJob.setMapperClass(KatzMapper.class);
			katzJob.setReducerClass(KatzReducer.class);
			katzJob.setSortComparatorClass(KatzSortComparator.class);
			
			katzJob.setMapOutputKeyClass(Text.class);
			katzJob.setMapOutputValueClass(Text.class);
			katzJob.setOutputKeyClass(Text.class);
			katzJob.setOutputValueClass(Text.class);
			katzJob.setNumReduceTasks(tasks);

			for (String path : inputPaths) {
				if (path != null) {
					System.out.println("input path--->" + path);
					FileInputFormat.addInputPath(katzJob, new Path(path));
				}
			}

			FileInputFormat.setInputDirRecursive(katzJob, true);
			FileSystem fs = FileSystem.get(conf);
			Path outputPath = new Path(backPath);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(katzJob, outputPath);

			katzJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			if (isLzo == 0) {
				setLzo(katzJob);
			}

			if (katzJob.waitForCompletion(true)) {
				System.out.println(katzJob.getJobName() + " successed");
			} else {
				System.out.println(katzJob.getJobName() + " failed");
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
