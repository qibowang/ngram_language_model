package cn.edu.blcu.nlp.goodTurning;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.hadoop.compression.lzo.LzoCodec;



public class CountOfCountsDriver {

	public static void main(String[] args) {
		int startOrder = 1;
		int endOrder = 3;
		String ngramCountPath = null;
		String countOfCountsPath = null;
		int isLzo = 0;// 等于0表示压缩

		boolean parameterValid=false;
		int parameterNum = args.length;
		String inputPaths[]=new String[10];
		int index=0;

		for (int i = 0; i < parameterNum; i++) {
			if (args[i].startsWith("-input")) {
				
				ngramCountPath = args[++i];
				if(index<inputPaths.length){
					inputPaths[index++]=ngramCountPath;
				}else{
					System.out.println("input paths are more than 10 please build the jar file again");
					parameterValid=true;
				}
				System.out.println("ngram count path--->"+ngramCountPath);
				
			} else if (args[i].equals("-coc")) {
				countOfCountsPath = args[++i];
				System.out.println("rawCountPath--->" + countOfCountsPath);
			} else if (args[i].equals("-startOrder")) {
				startOrder = Integer.parseInt(args[++i]);
				System.out.println("startOrder--->" + startOrder);
			} else if (args[i].equals("-endOrder")) {
				endOrder = Integer.parseInt(args[++i]);
				System.out.println("endOrder--->" + endOrder);
			} else if (args[i].equals("-isLzo")) {
				isLzo = Integer.parseInt(args[++i]);
				System.out.println("isLzo---->" + isLzo);
			} else {
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
			conf.setInt("startOrder", startOrder);
			conf.setInt("endOrder", endOrder);

			conf.setBoolean("mapreduce.compress.map.output", true);
			conf.setClass("mapreduce.map.output.compression.codec", LzoCodec.class, CompressionCodec.class);
			conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
			conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
			
			Job cocJob = Job.getInstance(conf, "count of counts job");
			System.out.println(cocJob.getJobName() + " is running!!!");
			cocJob.setJarByClass(CountOfCountsDriver.class);

			cocJob.setMapperClass(CountOfCountsMapper.class);
			cocJob.setReducerClass(CountOfCountsReducer.class);
			cocJob.setCombinerClass(CountOfCountsCombiner.class);
			
			cocJob.setMapOutputKeyClass(Text.class);
			cocJob.setMapOutputValueClass(IntWritable.class);
			cocJob.setOutputKeyClass(Text.class);
			cocJob.setOutputValueClass(LongWritable.class);
			cocJob.setNumReduceTasks(1);
			
			
			for(String path:inputPaths){
				if(path!=null){
					System.out.println("input path--->"+path);
					FileInputFormat.addInputPath(cocJob, new Path(path));
				}
			}
			
			FileInputFormat.setInputDirRecursive(cocJob, true);
			FileSystem fs = FileSystem.get(conf);
			Path outputPath = new Path(countOfCountsPath);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(cocJob, outputPath);
			cocJob.setInputFormatClass(SequenceFileInputFormat.class);
			cocJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			if (isLzo == 0) {
				setLzo(cocJob);
			}

			if (cocJob.waitForCompletion(true)) {
				System.out.println(cocJob.getJobName() + " successed");
			} else {
				System.out.println(cocJob.getJobName() + " failed");
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
