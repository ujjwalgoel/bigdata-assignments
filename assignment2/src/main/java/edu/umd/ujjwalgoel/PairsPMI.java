package edu.umd.ujjwalgoel; 
 
import java.io.IOException; 
import java.util.Arrays; 
import java.util.Iterator; 
import java.util.*; 
import java.io.BufferedReader; 
import java.io.InputStreamReader; 
import java.lang.Math; 
 
import org.apache.commons.cli.CommandLine; 
import org.apache.commons.cli.CommandLineParser; 
import org.apache.commons.cli.GnuParser; 
import org.apache.commons.cli.HelpFormatter; 
import org.apache.commons.cli.OptionBuilder; 
import org.apache.commons.cli.Options; 
import org.apache.commons.cli.ParseException; 
import org.apache.hadoop.conf.Configured; 
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.fs.FileStatus; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.FloatWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.WritableComparable; 
import org.apache.hadoop.io.WritableComparator; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.Partitioner; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; 
import org.apache.hadoop.util.Tool; 
import org.apache.hadoop.util.ToolRunner; 
import org.apache.log4j.Logger; 
 
import tl.lin.data.map.HMapStIW; 
import tl.lin.data.map.MapKI; 
import tl.lin.data.pair.PairOfStrings; 
import tl.lin.data.pair.PairOfWritables; 
import tl.lin.data.util.SequenceFileUtils; 
 
/** 
 ** @author Ujjwal Goel  
 **/ 
public class PairsPMI extends Configured implements Tool { 
  private static final Logger LOG = Logger.getLogger(PairsPMI.class); 
 
  private static class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> { 
    private static final Text KEY = new Text(); 
    private static final IntWritable ONE = new IntWritable(1); 
    private static final PairOfStrings PAIR = new PairOfStrings(); 
 
    @Override 
    public void map(LongWritable key, Text line, Context context) 
        throws IOException, InterruptedException { 
      String lineText = line.toString(); 
 
      StringTokenizer itr = new StringTokenizer(lineText); 
      HashSet<String> uniqueWords = new HashSet<String>(); 
      while(itr.hasMoreTokens()){ 
    	uniqueWords.add(itr.nextToken()); 
      }
 
      for (String word : uniqueWords) {  
        if (word.length() == 0) 
          continue; 
 
        for (String s : uniqueWords) { 
          if (s == word) 
            continue; 
 
          if (s.length() == 0) 
            continue; 
       
      	  PAIR.set(word, s); 
      	  context.write(PAIR, ONE); 
        } 
    
        PAIR.set(word, "*"); 
        context.write(PAIR, ONE); 
      } 
     } 
    } 
 
   protected static class MyPartitioner extends Partitioner<PairOfStrings, IntWritable> { 
     @Override 
     public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) { 
    	return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks; 
     } 
   } 
 
 
    public class KeyComparator extends WritableComparator{ 
      protected KeyComparator(){ 
        super(Text.class, true); 
      } 
 
    @SuppressWarnings("rawtypes") 
    @Override 
    public int compare(WritableComparable w1, WritableComparable w2){ 
        Text key1 = (Text)w1; 
        Text key2 = (Text)w2; 
        if(key1.toString() == "*"){ 
        return -1; 
        } 
        else if(key2.toString() == "*"){ 
        return 1; 
        } 
        else { 
        return key1.compareTo(key2); 
        } 
    } 
} 
 
 
  private static class MyReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> { 
    private final static IntWritable SUM = new IntWritable();  
 
    @Override 
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context) 
        throws IOException, InterruptedException { 
        int sum = 0; 
    	Iterator<IntWritable> iter = values.iterator(); 
        while (iter.hasNext()) { 
           sum += iter.next().get(); 
        } 
          if(sum < 10){ 
         	return; 
          } 
        SUM.set(sum); 
        context.write(key, SUM); 
      } 
   } 
 
  private static class MyMapper2 extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> { 
    private static final HMapStIW countMap = new HMapStIW(); 
    private static FloatWritable PMI = new FloatWritable(0); 
    private static final PairOfStrings WORDPAIR = new PairOfStrings(); 
    private static final IntWritable NXY = new IntWritable(0); 
    private static final IntWritable NX = new IntWritable(0); 
    private static final IntWritable NY = new IntWritable(0); 
    private static final IntWritable NTOTAL = new IntWritable(0); 
    private int totalCount = 0; 
 
    @Override 
      public void setup(Context context) { 
    	BufferedReader br = null; 
    	try { 
           FileSystem fs = FileSystem.get(new Configuration()); 
           FileStatus[] status = fs.listStatus(new Path("tempPairs")); 
           for(int i=0; i < status.length;i++){ 
              br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath()))); 
              String line=br.readLine(); 
                while (line != null){ 
                    String[] words = line.split("\\t"); 
                    String keyPair = words[0]; 
                    String value = words[1]; 
                    String[] pair = keyPair.split(","); 
                    line=br.readLine(); 
                    if (!pair[1].trim().startsWith("*")){ 
                       continue; 
                    }
		    int val = Integer.parseInt(value); 
            	    totalCount += val; 
            	    String word = pair[0].replaceAll("\\(", "").trim(); 
            	    countMap.put(word, val); 
            	} 
            } 
            NTOTAL.set(totalCount); 
       } catch(Exception ex) { 
       }  
    }         
 
    @Override 
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException { 
    	String[] words = value.toString().split("\\t"); 
        String keyPair = words[0]; 
        String countValue = words[1]; 
        String[] pair = keyPair.split(","); 
    	String word = pair[0].replaceAll("\\(", ""); 
 
        if (pair[1].trim().startsWith("*")){ 
           return; 
        } 
     
    	NX.set(countMap.get(word)); 
     
     	NXY.set(Integer.parseInt(countValue)); 
    	String secondWord = pair[1].trim().replaceAll("\\)", ""); 
    	NY.set(countMap.get(secondWord)); 
 
        WORDPAIR.set(word, secondWord); 
        PMI.set((float)Math.log10((float)((float)(NXY.get()*NTOTAL.get()))/((float)(NX.get()*NY.get())))); 
        context.write(WORDPAIR,PMI); 
    } 
  } 
     
 
  /** 
 *    * Creates an instance of this tool. 
 *       */ 
  public PairsPMI() {} 
 
  private static final String INPUT = "input"; 
  private static final String OUTPUT = "output"; 
  private static final String WINDOW = "window"; 
  private static final String NUM_REDUCERS = "numReducers"; 
 
  /** 
 *    * Runs this tool. 
 *       */ 
  @SuppressWarnings({ "static-access" }) 
  public int run(String[] args) throws Exception { 
    Options options = new Options(); 
 
    options.addOption(OptionBuilder.withArgName("path").hasArg() 
        .withDescription("input path").create(INPUT)); 
    options.addOption(OptionBuilder.withArgName("path").hasArg() 
        .withDescription("output path").create(OUTPUT)); 
    options.addOption(OptionBuilder.withArgName("num").hasArg() 
        .withDescription("number of reducers").create(NUM_REDUCERS)); 
 
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
    String outputPath = cmdline.getOptionValue(OUTPUT); 
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? 
        Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1; 
 
    LOG.info("Tool: " + PairsPMI.class.getSimpleName()); 
    LOG.info(" - input path: " + inputPath); 
    LOG.info(" - output path: " + outputPath); 
    LOG.info(" - number of reducers: " + reduceTasks); 
     
    getConf().set("mapred.child.java.opts", "-Xmx1024m");  
 
    Job job = new Job(getConf()); 
    job.setJobName(MyMapper.class.getSimpleName()); 
    job.setJarByClass(MyMapper.class); 
 
    Path outputDir = new Path("tempPairs"); 
    FileSystem.get(getConf()).delete(outputDir, true); 
 
    job.setNumReduceTasks(reduceTasks); 
 
    FileInputFormat.setInputPaths(job, new Path(inputPath)); 
    FileOutputFormat.setOutputPath(job, new Path("tempPairs")); 
 
    job.setMapOutputKeyClass(PairOfStrings.class); 
    job.setMapOutputValueClass(IntWritable.class); 
    job.setOutputKeyClass(PairOfStrings.class); 
    job.setOutputValueClass(IntWritable.class); 
    job.setOutputFormatClass(TextOutputFormat.class); 
 
    job.setMapperClass(MyMapper.class); 
    //job.setCombinerClass(MyReducer.class); 
    job.setReducerClass(MyReducer.class); 
    job.setPartitionerClass(MyPartitioner.class);  
 
    long startTime = System.currentTimeMillis(); 
    job.waitForCompletion(true); 
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds"); 
     
    Configuration conf = new Configuration(); 
    conf.set("mapred.child.java.opts", "-Xmx1024m"); 
    Job job2 = new Job(conf); 
    job2.setJobName(MyMapper2.class.getSimpleName()+"2"); 
    job2.setJarByClass(MyMapper2.class); 
 
    outputDir = new Path(outputPath); 
    FileSystem.get(conf).delete(outputDir, true); 
 
    job2.setNumReduceTasks(0); 
 
    FileInputFormat.setInputPaths(job2, new Path("tempPairs")); 
    FileOutputFormat.setOutputPath(job2, new Path(outputPath)); 
 
    job2.setMapOutputKeyClass(PairOfStrings.class); 
    job2.setMapOutputValueClass(FloatWritable.class); 
    job2.setOutputKeyClass(PairOfStrings.class); 
    job2.setOutputValueClass(FloatWritable.class); 
    job2.setOutputFormatClass(TextOutputFormat.class); 
     
    job2.setMapperClass(MyMapper2.class); 
 
    startTime = System.currentTimeMillis(); 
    job2.waitForCompletion(true); 
    System.out.println("Job 2 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds"); 
 
    return 0; 
  } 
 
  /** 
 *    * Dispatches command-line arguments to the tool via the {@code ToolRunner}. 
 *       */ 
  public static void main(String[] args) throws Exception { 
    ToolRunner.run(new PairsPMI(), args); 
  } 
}
