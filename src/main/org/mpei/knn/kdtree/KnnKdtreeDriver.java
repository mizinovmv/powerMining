package org.mpei.knn.kdtree;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.ResultAnalyzer;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.mpei.json.JsonInputFormat;
import org.mpei.knn.KnnDriver;
import org.mpei.knn.kdtree.tools.KDTree;
import org.mpei.knn.kdtree.tools.KnnKdtreeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KnnKdtreeDriver extends AbstractJob {

	private static final Logger log = LoggerFactory
			.getLogger(KnnKdtreeDriver.class);

	public static final String TOKEN_CASHE = "tokenCashe";
	public static final String TRAIN = "train";
	public static final String NEIGHBORS = "neighbors";
	public static int NN = 1;
	
	public static void main(String[] args) throws Exception {
		while (NN < 146) {
			ToolRunner.run(new Configuration(), new KnnKdtreeDriver(), args);
			++NN;
		}
	}

	public int run(String[] args) throws Exception {
		addInputOption();
		addOutputOption();
		addOption(DefaultOptionCreator.overwriteOption().create());
		addOption(KnnKdtreeDriver.TOKEN_CASHE, "tc",
				"Path with token's vocabulary.", "knnAnalyzer/part-r-00000");
		addOption(KnnKdtreeDriver.TRAIN, "t", "Path with train data.",
				"classes");
		addOption(KnnKdtreeDriver.NEIGHBORS, "nn",
				"Number of nearest neighbors.", "1");
		Map<String, List<String>> parsedArgs = parseArguments(args);
		if (parsedArgs == null) {
			return -1;
		}

		Path input = getInputPath();
		Path output = getOutputPath();
		if (hasOption(DefaultOptionCreator.OVERWRITE_OPTION)) {
			HadoopUtil.delete(getConf(), output);
		}

		Job job = HadoopUtil.prepareJob(input, output, JsonInputFormat.class,
				KnnKdtreeMapper.class, Text.class, IntWritable.class,
				IntSumReducer.class, Text.class, IntWritable.class,
				TextOutputFormat.class, getConf());
		job.setJobName("KnnKdtreeDriver");
		output.getFileSystem(getConf()).delete(output, true);

		job.getConfiguration().set(TOKEN_CASHE, getOption(TOKEN_CASHE));
		job.getConfiguration().set(TRAIN, getOption(TRAIN));
		job.getConfiguration().set(NEIGHBORS, String.valueOf(NN));
		
		Path train = new Path(job.getConfiguration().get(TRAIN));
		KDTree kdTree = KnnKdtreeBuilder.buildKDTree(train,
				job.getConfiguration());
		KnnKdtreeBuilder.writeKDTree(output, job.getConfiguration(), kdTree);	
		boolean succeeded = job.waitForCompletion(true);

		// log 
		LogGraphic(job,output);		
		//
		
		return succeeded ? 0 : -1;
	}
	
	private void logReadable(Job job,Path output) throws IOException{
//		String output = job.getConfiguration().get("mapred.dir.output");
		FileSystem fs = null;
		BufferedReader buffReader = null;
		BufferedWriter buffWriter = null;
		FileWriter fstream = new FileWriter("error.txt", true);
		try {
			fs = FileSystem.get(job.getConfiguration());
			FSDataInputStream fsIn = fs.open(new Path(output,
					"part-r-00000"));
			buffReader = new BufferedReader(new InputStreamReader(fsIn));
			buffWriter = new BufferedWriter(fstream);
			String line = null;
			int[] values = new int[2];
			int count = 0;
			while ((line = buffReader.readLine()) != null) {
				String[] tmp = line.split("\t");
				values[count] = Integer.valueOf(tmp[1]);
				++count;
			}

			StringBuilder returnString = new StringBuilder();
			int incorrectlyClassified = values[0];
			int correctlyClassified = values[1];
			returnString
					.append("=======================================================\n");
			returnString.append("Summary\n");
			returnString
					.append("-------------------------------------------------------\n");
			int totalClassified = correctlyClassified
					+ incorrectlyClassified;
			double percentageCorrect = (double) 100 * correctlyClassified
					/ totalClassified;
			double percentageIncorrect = (double) 100
					* incorrectlyClassified / totalClassified;
			NumberFormat decimalFormatter = new DecimalFormat("0.####");

			returnString
					.append(StringUtils.rightPad(
							"Num neihbors", 40))
					.append(": ")
					.append(StringUtils.leftPad(
							job.getConfiguration().get(NEIGHBORS), 10))
					.append("\n");
			returnString
					.append(StringUtils.rightPad(
							"Correctly Classified Instances", 40))
					.append(": ")
					.append(StringUtils.leftPad(
							Integer.toString(correctlyClassified), 10))
					.append('\t')
					.append(StringUtils.leftPad(
							decimalFormatter.format(percentageCorrect), 10))
					.append("%\n");
			returnString
					.append(StringUtils.rightPad(
							"Incorrectly Classified Instances", 40))
					.append(": ")
					.append(StringUtils.leftPad(
							Integer.toString(incorrectlyClassified), 10))
					.append('\t')
					.append(StringUtils.leftPad(
							decimalFormatter.format(percentageIncorrect),
							10)).append("%\n");
			returnString
					.append(StringUtils.rightPad(
							"Total Classified Instances", 40))
					.append(": ")
					.append(StringUtils.leftPad(
							Integer.toString(totalClassified), 10))
					.append('\n');
			returnString.append('\n');
			buffWriter.write(returnString.toString());
		} catch (Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
		} finally {
			buffReader.close();
			buffWriter.close();
			fs.close();
		}
	}
	
	private void LogGraphic(Job job,Path output) throws IOException{
//		String output = job.getConfiguration().get("mapred.dir.output");
		FileSystem fs = null;
		BufferedReader buffReader = null;
		BufferedWriter buffWriter = null;
		FileWriter fstream = new FileWriter("error.txt",true);
		try {
			fs = FileSystem.get(job.getConfiguration());
			FSDataInputStream fsIn = fs.open(new Path(output,"part-r-00000"));
			buffReader = new BufferedReader(new InputStreamReader(fsIn));
			buffWriter = new BufferedWriter(fstream);
			String line = null;
			double[] values = new double[2];
			int count = 0;
			while ((line = buffReader.readLine()) != null) {
				String[] tmp = line.split("\t");
				values[count] = Double.valueOf(tmp[1]);
				++count;
			}
			buffWriter.write(String.valueOf(NN) + "\t" +String.valueOf(values[0]/(values[0] + values[1]))+"\n");
		} catch (Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
		} finally {
			buffReader.close();
			buffWriter.close();
			fs.close();
		}
	}
}
