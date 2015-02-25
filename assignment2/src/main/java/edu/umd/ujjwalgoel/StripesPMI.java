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
 * @author Ujjwal Goel  
 **/ 
public class StripesPMI extends Configured implements Tool { 
  private static final Logger LOG = Logger.getLogger(StripesPMI.class); 
 
  private static class MyMapper extends Mapper<LongWritable, Text, Text, HMapStIW> { 
    private static final HMapStIW MAP = new HMapStIW(); 
    private static final HMapStIW AGGREGATEMAP = new HMapStIW(); 
    private static final Text KEY = new Text(); 
 
    @Override 
    public void map(LongWritable key, Text line, Context context) 
        throws IOException, InterruptedException { 
      String lineText = line.toString(); 
      AGGREGATEMAP.clear(); 
      StringTokenizer itr = new StringTokenizer(lineText); 
      HashSet<String> uniqueWords = new HashSet<String>(); 
      while(itr.hasMoreTokens()){ 
         uniqueWords.add(itr.nextToken()); 
      } 
      for (String word : uniqueWords) { 
        if (word.length() == 0) 
          continue; 
 
        MAP.clear(); 
 
        for (String s : uniqueWords) { 
          if (s == word) 
            continue; 
           
          if (s.length() == 0) 
            continue; 
           
          MAP.increment(s); 
        } 
    
        MAP.increment("*"); 
        KEY.set(word); 
        context.write(KEY, MAP); 
        AGGREGATEMAP.increment(word); 
	AGGREGATEMAP.increment("*");
      }  
      KEY.set("*"); 
      context.write(KEY, AGGREGATEMAP); 
     } 
    } 
 
protected static class MyPartitioner extends Partitioner<Text,HMapStIW > { 
    @Override 
    public int getPartition(Text key, HMapStIW value, int numReduceTasks) { 
      if(key.toString() == "*") { return 0;} 
      return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks; 
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
 
 
  private static class MyReducer extends Reducer<Text, HMapStIW, Text, HMapStIW> { 
    private static final HMapStIW map = new HMapStIW(); 
    private static final HMapStIW finalMap = new HMapStIW();

    @Override 
    public void reduce(Text key, Iterable<HMapStIW> values, Context context) 
        throws IOException, InterruptedException { 
      Iterator<HMapStIW> iter = values.iterator(); 
      map.clear();
      finalMap.clear(); 
      while (iter.hasNext()) { 
        map.plus(iter.next()); 
      }
      Set<MapKI.Entry<String>> entries = map.entrySet();
      for(MapKI.Entry<String> pair : entries) {
	 if(pair.getValue() >=10) {
	    finalMap.put(pair.getKey(), pair.getValue());
         }
      }
      if(finalMap.size() > 0 ){
        context.write(key, finalMap);
      }
    } 
  } 
 
  private static class MyMapper2 extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> { 
    private static final HMapStIW countMap = new HMapStIW(); 
    private static final HMapStIW localMap = new HMapStIW();  
    private static FloatWritable PMI = new FloatWritable(0); 
    private static final PairOfStrings WORDPAIR = new PairOfStrings(); 
    private static final IntWritable NXY = new IntWritable(0); 
    private static final IntWritable NX = new IntWritable(0); 
    private static final IntWritable NY = new IntWritable(0); 
    private static final IntWritable NTOTAL = new IntWritable(0); 
    private static int count = 0; 
 
    @Override 
      public void setup(Context context) { 
    BufferedReader br = null; 
    boolean found = false; 
    try {  
        FileSystem fs = FileSystem.get(new Configuration()); 
        FileStatus[] status = fs.listStatus(new Path("tempStripes")); 
        for(int i=0; i < status.length;i++){ 
          if(found){ 
            break; 
          } 
            br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath()))); 
            String line=br.readLine(); 
            while (line != null){ 
                if(!line.startsWith("*")){ 
                  line = br.readLine(); 
                  continue; 
                 } 
                String[] words = line.split("\\t"); 
                String valueWord = words[1]; 
                valueWord = valueWord.replaceAll("\\{",""); 
                valueWord = valueWord.replaceAll("\\}",""); 
                String[] kvs = valueWord.split(","); 
                //line = br.readLine();
		for(String kv : kvs) { 
                   kv = kv.trim(); 
                   String[] keyValues = kv.split("=");
		   //if(keyValues[0].trim() == "*"){
		     int val = Integer.parseInt(keyValues[1].trim());
		     //count += val;
              	     countMap.put(keyValues[0].trim(), val);
		     //break;
                   //}
                } 
                found = true; 
                break; 
            } 
         } 
	//NTOTAL.set(count);
    } catch(Exception ex) {   
        }  
      }         
 
 
    @Override 
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException { 
     
    String[] words = value.toString().split("\\t"); 
    String keyWord = words[0]; 
    String valueWord = words[1]; 
    if (keyWord.toString().equals("*")){ 
        return; 
    } 
    localMap.clear(); 
    valueWord = valueWord.replaceAll("\\{",""); 
    valueWord = valueWord.replaceAll("\\}",""); 
        String[] kvs = valueWord.split(","); 
        for(String kv : kvs) { 
            kv = kv.trim(); 
            String[] keyValues = kv.split("="); 
            int val = Integer.parseInt(keyValues[1].trim()); 
            localMap.put(keyValues[0].trim(), val); 
        } 
 
    NX.set(localMap.get("*")); 
    if(NX.get() == 0){ 
       System.out.println("NX is zero!!!"); 
    } 
     
    Set<MapKI.Entry<String>> entries = localMap.entrySet(); 
    for(MapKI.Entry<String> pair : entries){ 
       if(pair.getKey().equals("*")){ 
        continue; 
       } 
       NXY.set(pair.getValue()); 
       NY.set(countMap.get(pair.getKey())); 
 
       NTOTAL.set(countMap.get("*")); 
       WORDPAIR.set(keyWord, pair.getKey()); 
       PMI.set((float)Math.log10((float)((float)(NXY.get()*NTOTAL.get()))/((float)(NX.get()*NY.get())))); 
       context.write(WORDPAIR,PMI); 
    } 
     } 
  } 
     
 
  /** 
 *    * Creates an instance of this tool. 
 *       */ 
  public StripesPMI() {} 
 
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
 
    LOG.info("Tool: " + StripesPMI.class.getSimpleName()); 
    LOG.info(" - input path: " + inputPath); 
    LOG.info(" - output path: " + outputPath); 
    LOG.info(" - number of reducers: " + reduceTasks); 
     
    getConf().set("mapred.child.java.opts", "-Xmx1024m"); 
 
    Job job = new Job(getConf()); 
    job.setJobName(MyMapper.class.getSimpleName()); 
    job.setJarByClass(MyMapper.class); 
 
    Path outputDir = new Path("tempStripes"); 
    FileSystem.get(getConf()).delete(outputDir, true); 
 
    job.setNumReduceTasks(reduceTasks); 
 
    FileInputFormat.setInputPaths(job, new Path(inputPath)); 
    FileOutputFormat.setOutputPath(job, new Path("tempStripes")); 
 
    job.setMapOutputKeyClass(Text.class); 
    job.setMapOutputValueClass(HMapStIW.class); 
    job.setOutputKeyClass(Text.class); 
    job.setOutputValueClass(HMapStIW.class); 
    job.setOutputFormatClass(TextOutputFormat.class); 
 
    job.setMapperClass(MyMapper.class); 
    //job.setCombinerClass(MyReducer.class); 
    job.setReducerClass(MyReducer.class);  
 
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
 
    FileInputFormat.setInputPaths(job2, new Path("tempStripes")); 
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
    ToolRunner.run(new StripesPMI(), args); 
  } 
}
