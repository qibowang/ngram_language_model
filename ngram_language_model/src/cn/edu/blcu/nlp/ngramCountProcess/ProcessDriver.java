package cn.edu.blcu.nlp.ngramCountProcess;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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



public class ProcessDriver {
	public static void main(String[] args) {
		int tasks = 1;// 设置为7
		
		String inputPath = null;
		String mergePath = null;
		String inputPaths[]=new String[10];
		
		
		int isLzo = 0;// 等于0表示压缩
		int index=0;
		int startOrder=1;
		int endOrder=5;
		
		for (int i = 0; i < args.length; i++) {
			
			if (args[i].startsWith("-input")) {
				inputPath = args[++i];
				if(index<inputPaths.length){
					inputPaths[index++]=inputPath;
				}else{
					System.out.println("input paths are more than 10 please build the jar file again");
				}
				System.out.println("rawCountPath--->" + inputPath);
			}  else if (args[i].equals("-merge")) {
				mergePath = args[++i];
				System.out.println("mergePath--->" + mergePath);
			} else if (args[i].equals("-tasks")) {
				tasks = Integer.parseInt(args[++i]);
				System.out.println("tasks--->" + tasks);
			}  else if (args[i].equals("-isLzo")) {
				isLzo = Integer.parseInt(args[++i]);
				System.out.println("isLzo---->" + isLzo);
			}  else if(args[i].equals("-startOrder")){
				startOrder=Integer.parseInt(args[++i]);
				System.out.println("startOrder--->"+startOrder);
			}else if(args[i].equals("-endOrder")){
				endOrder=Integer.parseInt(args[++i]);
				System.out.println("endOrder--->"+endOrder);
			}else {
				System.out.println("there exists invalid parameters--->" + args[i]);
				break;
			}
			
		}

		try {
			Path outputPath = null;
			Configuration conf = new Configuration();
			
			
			conf.setBoolean("mapreduce.compress.map.output", true);
			conf.setClass("mapreduce.map.output.compression.codec", LzoCodec.class, CompressionCodec.class);
			
			conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
			conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
			
			conf.setInt("startOrder", startOrder);
			conf.setInt("endOrder", endOrder);
			
			FileSystem fs = FileSystem.get(conf);
			
			fs = FileSystem.get(conf);
			Job mergeJob = Job.getInstance(conf, "Ngram count merge job");
			System.out.println(mergeJob.getJobName() + " is running!!!");
			mergeJob.setJarByClass(ProcessDriver.class);

			mergeJob.setMapperClass(ProcessMapper.class);
			mergeJob.setReducerClass(ProcessReducer.class);
			mergeJob.setCombinerClass(ProcessReducer.class);

			mergeJob.setMapOutputKeyClass(Text.class);
			mergeJob.setMapOutputValueClass(LongWritable.class);
			mergeJob.setOutputKeyClass(Text.class);
			mergeJob.setOutputValueClass(LongWritable.class);

			mergeJob.setInputFormatClass(SequenceFileInputFormat.class);
			mergeJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			
			mergeJob.setNumReduceTasks(tasks);
			
			for(String path:inputPaths){
				if(path!=null){
					System.out.println("input path--->"+path);
					FileInputFormat.addInputPath(mergeJob, new Path(path));
				}
			}
			
			FileInputFormat.setInputDirRecursive(mergeJob, true);
			outputPath = new Path(mergePath);
			
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}			
			FileOutputFormat.setOutputPath(mergeJob, outputPath);
			mergeJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			if (isLzo == 0) {
				setLzo(mergeJob);
			}
			if (mergeJob.waitForCompletion(true)) {
				System.out.println("rawcount merge step successed!");
			} else {
				System.out.println("rawcount merge step failed!");
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
