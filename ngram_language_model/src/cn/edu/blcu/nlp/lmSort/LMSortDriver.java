package cn.edu.blcu.nlp.lmSort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hadoop.compression.lzo.LzoCodec;

public class LMSortDriver {
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
			Job sortJob = Job.getInstance(conf, "sort Job");
			System.out.println(sortJob.getJobName() + " is running!");
			sortJob.setJarByClass(LMSortDriver.class);
			sortJob.setMapperClass(LMSortMapper.class);
			sortJob.setReducerClass(LMSortReducer.class);
			sortJob.setSortComparatorClass(LMSortComparator.class);
			sortJob.setNumReduceTasks(1);

			sortJob.setInputFormatClass(SequenceFileInputFormat.class);
			sortJob.setMapOutputKeyClass(Text.class);
			sortJob.setMapOutputValueClass(Text.class);
			sortJob.setOutputKeyClass(Text.class);
			sortJob.setOutputValueClass(Text.class);

			for(String path:inputPaths){
				if(path!=null){
					System.out.println("input path--->"+path);
					FileInputFormat.addInputPath(sortJob, new Path(path));
				}
			}
			
			FileInputFormat.setInputDirRecursive(sortJob, true);
			FileSystem fs = FileSystem.get(conf);
			Path outputPath = new Path(output);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(sortJob, outputPath);

			if (sortJob.waitForCompletion(true)) {
				System.out.println(sortJob.getJobName() + " Job successed");
			} else {
				System.out.println(sortJob.getJobName() + " Job failed");
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
