package com.google.cloud.hadoop.io.bigquery.samples;

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryOutputFormat;
import com.google.cloud.hadoop.io.bigquery.GsonBigQueryInputFormat;
import com.google.cloud.hadoop.util.LogUtil;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Sample program to run the Hadoop Wordcount example over tables in BigQuery.
 */
public class WordCount {
  // Logger.
  protected static final LogUtil log = new LogUtil(WordCount.class);

  // Configurable schema field name identifying the field in each BigQuery record for which we are
  // counting words. Examples: 'word' in publicdata:samples.shakespeare or 'repository_name'
  // in publicdata:samples.github_timeline.
  public static final String WORDCOUNT_WORD_KEY = "mapred.bq.samples.wordcount.word.key";

  // Default value of WORDCOUNT_WORD_KEY.
  public static final String WORDCOUNT_WORD_KEY_DEFAULT = "word";

  // Guava might not be available, so define a null / empty helper:
  private static boolean isStringNullOrEmpty(String toTest) {
    return toTest == null || "".equals(toTest);
  }

  /**
   * The mapper function for word count.
   */
  public static class Map 
      extends Mapper <LongWritable, JsonObject, Text, LongWritable> {
    private static final LongWritable oneWritable = new LongWritable(1);
    private Text word = new Text();
    private String wordKey;

    @Override
    public void setup(Context context)
        throws IOException, InterruptedException {
      // Find the runtime-configured key for the field name we're looking for in the map task.
      wordKey = context.getConfiguration().get(WORDCOUNT_WORD_KEY, WORDCOUNT_WORD_KEY_DEFAULT);
    }

    @Override
    public void map(LongWritable key, JsonObject value, Context context)
        throws IOException, InterruptedException {
      JsonElement countElement = value.get(wordKey);
      if (countElement != null) {
        String wordInRecord = countElement.getAsString();
        word.set(wordInRecord);
        context.write(word, oneWritable);
      }
    }
  }

  /**
   * Reducer function for word count.
   */
  public static class Reduce
      extends Reducer<Text, LongWritable, Text, JsonObject> {
    private static final Text dummyText = new Text("ignored");

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
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

  /**
   * Configures and runs the main Hadoop job. Takes a String[] of 4 parameters
   *
   * @param args a String[] containing projectId, fullyQualifiedInputTableId,
   *     and fullyQualifiedOutputTableId.
   * @throws IOException on IO Error.
   * @throws InterruptedException on Interrupt.
   * @throws ClassNotFoundException if not all classes are present.
   */
  public static void main(String[] args)
      throws IOException, InterruptedException, ClassNotFoundException {

    GenericOptionsParser parser = new GenericOptionsParser(args);
    args = parser.getRemainingArgs();

    if (args.length != 4) {
      System.out.println("Usage: hadoop jar bigquery_wordcount.jar "
          + "[projectId] [fullyQualifiedInputTableId] [fieldName] [fullyQualifiedOutputTableId]");
      String indent = "    ";
      System.out.println(indent
          + "projectId - Project under which to issue the BigQuery operations. "
          + "Also serves as the default project for table IDs which don't explicitly specify a "
          + "project for the table.");
      System.out.println(indent
          + "fullyQualifiedInputTableId - Input table ID of the form "
          + "<optional projectId>:<datasetId>.<tableId>");
      System.out.println(indent
          + "fieldName - Name of the field to count in the input table, e.g. 'word' in "
          + "publicdata:samples.shakespeare or 'repository_name' in "
          + "publicdata:samples.github_timeline.");
      System.out.println(indent
          + "fullyQualifiedOutputTableId - Output table ID of the form "
          + "<optional projectId>:<datasetId>.<tableId>");
      System.exit(1);
    }

    // Global parameters from args.
    String projectId = args[0];

    // Set InputFormat parameters from args.
    String fullyQualifiedInputTableId = args[1];
    String fieldName = args[2];

    // Set OutputFormat parameters from args.
    String fullyQualifiedOutputTableId = args[3];

    // Default OutputFormat parameters for this sample.
    String outputTableSchema =
        "[{'name': 'Word','type': 'STRING'},{'name': 'Count','type': 'INTEGER'}]";
    String jobName = "wordcount";

    JobConf conf = new JobConf(parser.getConfiguration(), WordCount.class);

    // Set the job-level projectId.
    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId);

    // Make sure the required export-bucket setting is present.
    if (isStringNullOrEmpty(conf.get(BigQueryConfiguration.GCS_BUCKET_KEY))) {
      log.warn("Missing config for '%s'; trying to default to fs.gs.system.bucket.",
               BigQueryConfiguration.GCS_BUCKET_KEY);
      String systemBucket = conf.get("fs.gs.system.bucket");
      if (isStringNullOrEmpty(systemBucket)) {
        log.error("Also missing fs.gs.system.bucket; value must be specified.");
        System.exit(1);
      } else {
        log.info("Setting '%s' to '%s'", BigQueryConfiguration.GCS_BUCKET_KEY, systemBucket);
        conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, systemBucket);
      }
    } else {
      log.info("Using export bucket '%s' as specified in '%s'",
          conf.get(BigQueryConfiguration.GCS_BUCKET_KEY), BigQueryConfiguration.GCS_BUCKET_KEY);
    }

    // Configure input and output for BigQuery acccess.
    BigQueryConfiguration.configureBigQueryInput(conf, fullyQualifiedInputTableId);
    BigQueryConfiguration.configureBigQueryOutput(
        conf, fullyQualifiedOutputTableId, outputTableSchema);

    // Set the field we're querying for the count.
    log.info("Setting '%s' to '%s'", WORDCOUNT_WORD_KEY, fieldName);
    conf.set(WORDCOUNT_WORD_KEY, fieldName);

    Job job = new Job(conf, jobName);
    job.setJarByClass(WordCount.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    // Set input and output classes.
    job.setInputFormatClass(GsonBigQueryInputFormat.class);
    job.setOutputFormatClass(BigQueryOutputFormat.class);

    job.waitForCompletion(true);

    // Make sure to clean up the Google Cloud Storage export paths.
    GsonBigQueryInputFormat.cleanupJob(job);
  }
}
