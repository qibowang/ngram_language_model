package cn.edu.blcu.nlp.goodTurning;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.hadoop.compression.lzo.LzoCodec;

public class GoodTurningDriver {
	public static void main(String[] args) {
		String cocPath = "";
		String ngramCountPath = "";
		String gtProbPath = "";
		
		int gtmin = 3;
		int startOrder = 1;
		int endOrder = 5;
		int isLzo = 0;
		int tasks = 1;

		boolean parameterValid = false;
		int parameterNum = args.length;
		String inputPaths[] = new String[10];
		int index = 0;

		for (int i = 0; i < parameterNum; i++) {
			if (args[i].equals("-input")) {
				ngramCountPath = args[++i];
				if (index < inputPaths.length) {
					inputPaths[index++] = ngramCountPath;
				} else {
					System.out.println("input paths are more than 10 please build the jar file again");
					parameterValid = true;
				}
				System.out.println("ngram count path--->" + ngramCountPath);

			} else if (args[i].equals("-output")) {
				gtProbPath = args[++i];
				System.out.println("gtProbPath--->" + gtProbPath);
			} else if (args[i].equals("-startOrder")) {
				startOrder = Integer.parseInt(args[++i]);
				System.out.println("startOrder--->" + startOrder);
			} else if (args[i].equals("-endOrder")) {
				endOrder = Integer.parseInt(args[++i]);
				System.out.println("endOrder--->" + endOrder);
			} else if (args[i].equals("-isLzo")) {
				isLzo = Integer.parseInt(args[++i]);
				System.out.println("isLzo---->" + isLzo);
			} else if (args[i].equals("-tasks")) {
				tasks = Integer.parseInt(args[++i]);
				System.out.println("tasks--->" + tasks);
			} else if (args[i].equals("-gtmin")) {
				gtmin = Integer.parseInt(args[++i]);
				System.out.println("gtmin--->" + gtmin);
			} else if (args[i].equals("-coc")) {
				cocPath = args[++i];
				System.out.println("cocPath--->" + cocPath);
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
			conf.setInt("startOrder", startOrder);
			conf.setInt("endOrder", endOrder);
			conf.setInt("gtmin", gtmin);
			conf.set("cocPath", cocPath);
			conf.setBoolean("mapreduce.compress.map.output", true);
			conf.setClass("mapreduce.map.output.compression.codec", LzoCodec.class, CompressionCodec.class);
			conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
			conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");

			Job goodTurningProbJob = Job.getInstance(conf, "Good-Turning Prob job");
			System.out.println(goodTurningProbJob.getJobName() + " is running!!!");
			goodTurningProbJob.setJarByClass(CountOfCountsDriver.class);

			goodTurningProbJob.setMapperClass(GoodTurningMapper.class);
			goodTurningProbJob.setReducerClass(GoodTurningReducer.class);

			goodTurningProbJob.setMapOutputKeyClass(Text.class);
			goodTurningProbJob.setMapOutputValueClass(Text.class);
			goodTurningProbJob.setOutputKeyClass(Text.class);
			goodTurningProbJob.setOutputValueClass(Text.class);
			goodTurningProbJob.setNumReduceTasks(tasks);

			for (String path : inputPaths) {
				if (path != null) {
					System.out.println("input path--->" + path);
					FileInputFormat.addInputPath(goodTurningProbJob, new Path(path));
				}
			}

			FileInputFormat.setInputDirRecursive(goodTurningProbJob, true);
			FileSystem fs = FileSystem.get(conf);
			Path outputPath = new Path(gtProbPath);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(goodTurningProbJob, outputPath);
			goodTurningProbJob.setInputFormatClass(SequenceFileInputFormat.class);
			goodTurningProbJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			if (isLzo == 0) {
				setLzo(goodTurningProbJob);
			}

			if (goodTurningProbJob.waitForCompletion(true)) {
				System.out.println(goodTurningProbJob.getJobName() + " successed");
			} else {
				System.out.println(goodTurningProbJob.getJobName() + " failed");
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
