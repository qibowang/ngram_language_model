package cn.edu.blcu.nlp.probJoinKatz;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.hadoop.compression.lzo.LzoCodec;

public class JoinDriver {
	public static void main(String[] args) {
		String probPath = "";
		String backPath = "";
		String lmPath = "";
		int isLzo = 0;
		int tasks = 7;
		int gtmin = 3;

		boolean parameterValid = false;
		int parameterNum = args.length;

		for (int i = 0; i < parameterNum; i++) {
			if (args[i].startsWith("-prob")) {
				probPath = args[++i];
				System.out.println("prob path--->" + probPath);
			} else if (args[i].startsWith("-back")) {
				backPath = args[++i];
				System.out.println("back path--->" + backPath);
			} else if (args[i].equals("-output")) {
				lmPath = args[++i];
				System.out.println("lmPath--->" + lmPath);
			} else if (args[i].equals("-isLzo")) {
				isLzo = Integer.parseInt(args[++i]);
				System.out.println("isLzo---->" + isLzo);
			} else if (args[i].equals("-tasks")) {
				tasks = Integer.parseInt(args[++i]);
				System.out.println("tasks--->" + tasks);
			} else if (args[i].equals("-gtmin")) {
				gtmin = Integer.parseInt(args[++i]);
				System.out.println("gtmin--->" + gtmin);
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
			conf.setInt("gtmin", gtmin);
			
			Job probJoinBackJob = Job.getInstance(conf, "prob join back");
			System.out.println(probJoinBackJob.getJobName() + " is running!!!");

			probJoinBackJob.setJarByClass(JoinDriver.class);
			probJoinBackJob.setReducerClass(JoinReducer.class);
			probJoinBackJob.setNumReduceTasks(tasks);
			probJoinBackJob.setSortComparatorClass(ProbJoinBackSort.class);

			probJoinBackJob.setMapOutputKeyClass(Text.class);
			probJoinBackJob.setMapOutputValueClass(Text.class);
			probJoinBackJob.setOutputKeyClass(Text.class);
			probJoinBackJob.setOutputValueClass(Text.class);

			MultipleInputs.addInputPath(probJoinBackJob, new Path(probPath), SequenceFileInputFormat.class, ProbMapper.class);
			MultipleInputs.addInputPath(probJoinBackJob, new Path(backPath), SequenceFileInputFormat.class, KatzMapper.class);

			FileInputFormat.setInputDirRecursive(probJoinBackJob, true);
			FileSystem fs = FileSystem.get(conf);
			Path outputPath = new Path(lmPath);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(probJoinBackJob, outputPath);

			probJoinBackJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			if (isLzo == 0) {
				setLzo(probJoinBackJob);
			}

			if (probJoinBackJob.waitForCompletion(true)) {
				System.out.println(probJoinBackJob.getJobName() + " successed");
			} else {
				System.out.println(probJoinBackJob.getJobName() + " failed");
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
