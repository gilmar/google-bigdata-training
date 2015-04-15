package com.ciandt.hadoop.examples;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryOutputFormat;
import com.google.cloud.hadoop.io.bigquery.GsonBigQueryInputFormat;
import com.google.cloud.hadoop.util.LogUtil;
import com.google.gson.JsonObject;

public class WordCountToBigQuery {

	// Logger.
	protected static final LogUtil log = new LogUtil(WordCountToBigQuery.class);

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text wordText = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			List<String> words = Arrays.asList(line.split("\\s"));
			for (String word : words) {
				wordText.set(word);
				context.write(wordText, one);
			}
		}
	}

	/**
	 * Reducer function for word count.
	 */
	public static class Reduce extends Reducer<Text, LongWritable, Text, JsonObject> {
		private static final Text dummyText = new Text("ignored");

		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,
				InterruptedException {
			JsonObject jsonObject = new JsonObject();
			jsonObject.addProperty("Word", key.toString());
			long count = 0;
			for (LongWritable val : values) {
				count = count + val.get();
			}
			jsonObject.addProperty("Count", count);
			// Key does not matter.
			context.write(dummyText, jsonObject);
		}
	}

	public static void main(String[] args) throws Exception {

		GenericOptionsParser parser = new GenericOptionsParser(args);
		args = parser.getRemainingArgs();

		if (args.length != 3) {
			System.out.println("Usage: hadoop google-bigdata-training-job.jar "
					+ "[projectId] [inputPath] [fullyQualifiedOutputTableId]");
			String indent = "    ";
			System.out.println(indent + "projectId - Project under which to issue the BigQuery operations. "
					+ "Also serves as the default project for table IDs which don't explicitly specify a "
					+ "project for the table.");
			System.out.println(indent + "inputPath - m " + "<optional projectId>:<datasetId>.<tableId>");
			System.out.println(indent + "fullyQualifiedOutputTableId - Output table ID of the form "
					+ "<optional projectId>:<datasetId>.<tableId>");
			System.exit(1);
		}

		// Global parameters from args.
		String projectId = args[0];

		String inputPath = args[1];

		// Set OutputFormat parameters from args.
		String fullyQualifiedOutputTableId = args[2];
		
		// Default OutputFormat parameters for this sample.
		String outputTableSchema = "[{'name': 'Word','type': 'STRING'},{'name': 'Count','type': 'INTEGER'}]";

		JobConf conf = new JobConf(parser.getConfiguration(), WordCountToBigQuery.class);

		// Set the job-level projectId.
		conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId);

		// Make sure the required export-bucket setting is present.
		if (StringUtils.isNotEmpty(conf.get(BigQueryConfiguration.GCS_BUCKET_KEY))) {
			log.warn("Missing config for '%s'; trying to default to fs.gs.system.bucket.",
					BigQueryConfiguration.GCS_BUCKET_KEY);
			String systemBucket = conf.get("fs.gs.system.bucket");
			if (StringUtils.isNotEmpty(systemBucket)) {
				log.error("Also missing fs.gs.system.bucket; value must be specified.");
				System.exit(1);
			} else {
				log.info("Setting '%s' to '%s'", BigQueryConfiguration.GCS_BUCKET_KEY, systemBucket);
				conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, systemBucket);
			}
		} else {
			log.info("Using export bucket '%s' as specified in '%s'", conf.get(BigQueryConfiguration.GCS_BUCKET_KEY),
					BigQueryConfiguration.GCS_BUCKET_KEY);
		}

		// Configure output for BigQuery acccess.
		BigQueryConfiguration.configureBigQueryOutput(conf, fullyQualifiedOutputTableId, outputTableSchema);

		Job job = new Job(conf, "wordcounttobq");

	    job.setJarByClass(WordCountToBigQuery.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

	    // Set input and output classes
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(BigQueryOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));

		job.waitForCompletion(true);
		
	    // Make sure to clean up the Google Cloud Storage export paths.
	    GsonBigQueryInputFormat.cleanupJob(conf,job.getJobID());
	}
}
