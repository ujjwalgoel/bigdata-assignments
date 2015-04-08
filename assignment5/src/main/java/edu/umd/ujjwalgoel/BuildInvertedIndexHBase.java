package edu.umd.ujjwalgoel;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfWritables;

public class BuildInvertedIndexHBase extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexHBase.class);
  public static final String[] FAMILIES = { "p" };
  public static final byte[] CF = FAMILIES[0].getBytes();
 
  private static class MyMapper extends Mapper<LongWritable, Text, Text, PairOfInts> {
    private static final Text WORD = new Text();
    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<String>();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      String text = doc.toString();
      COUNTS.clear();

      String[] terms = text.split("\\s+");

      // First build a histogram of the terms.
      for (String term : terms) {
        if (term == null || term.length() == 0) {
          continue;
        }

        COUNTS.increment(term);
      }

      // Emit postings.
      for (PairOfObjectInt<String> e : COUNTS) {
	WORD.set(e.getLeftElement());
	context.write(WORD, new PairOfInts((int) docno.get(), e.getRightElement()));
      }
    }
  }

  private static class MyReducer extends TableReducer<Text, PairOfInts, ImmutableBytesWritable> {
    private final static IntWritable DF = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<PairOfInts> values, Context context)
        throws IOException, InterruptedException {
      Iterator<PairOfInts> iter = values.iterator();
      ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();

      int df = 0;
      while (iter.hasNext()) {
        PairOfInts pair = iter.next();
        Put put = new Put(Bytes.toBytes(key.toString()));
        put.add(CF, Bytes.toBytes(pair.getLeftElement()), Bytes.toBytes(pair.getRightElement()));
        context.write(null, put);
     }
  }
}

  private BuildInvertedIndexHBase() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("HBase table name").create(OUTPUT));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputTable = cmdline.getOptionValue(OUTPUT);

    // If the table doesn't already exist, create it.
    Configuration conf = getConf();
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

    Configuration hbaseConfig = HBaseConfiguration.create(conf);
    HBaseAdmin admin = new HBaseAdmin(hbaseConfig);

    if (admin.tableExists(outputTable)) {
      LOG.info(String.format("Table '%s' exists: dropping table and recreating.", outputTable));
      LOG.info(String.format("Disabling table '%s'", outputTable));
      admin.disableTable(outputTable);
      LOG.info(String.format("Droppping table '%s'", outputTable));
      admin.deleteTable(outputTable);
    }

     HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(outputTable));
     for (int i = 0; i < FAMILIES.length; i++) {
        HColumnDescriptor hColumnDesc = new HColumnDescriptor(FAMILIES[i]);
        tableDesc.addFamily(hColumnDesc);
    }
    
    admin.createTable(tableDesc);
    LOG.info(String.format("Successfully created table '%s'", outputTable));

    admin.close();

    LOG.info("Tool name: " + BuildInvertedIndexHBase.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output table: " + outputTable);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexHBase.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexHBase.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PairOfInts.class);

    job.setMapperClass(MyMapper.class);
    job.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    job.setInputFormatClass(TextInputFormat.class);
 
    TableMapReduceUtil.initTableReducerJob(outputTable, MyReducer.class, job);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexHBase(), args);
  }
}
