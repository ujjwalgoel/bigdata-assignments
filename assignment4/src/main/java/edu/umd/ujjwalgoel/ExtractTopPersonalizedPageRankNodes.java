package edu.umd.ujjwalgoel;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfWritables;

import java.util.PriorityQueue;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import edu.umd.cloud9.io.SequenceFileUtils;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
  private static final String INPUT = "input";
  private static final String SOURCES = "sources";
  private static final String TOP = "top";

  public ExtractTopPersonalizedPageRankNodes() {
  }

  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("sources").hasArg()
        .withDescription("source nodes").create(SOURCES));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("top n").create(TOP));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(SOURCES) || !cmdline.hasOption(TOP)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(ExtractTopPersonalizedPageRankNodes.class.getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(-1);
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String sources = cmdline.getOptionValue(SOURCES);
    int n = Integer.parseInt(cmdline.getOptionValue(TOP));

    List<PageRankNode> nodes = new ArrayList<PageRankNode>();
    try {
      FileSystem fs = FileSystem.get(getConf());
      Path path = new Path(inputPath);
      FileStatus[] stat = fs.listStatus(path);
      BufferedReader br = null;
      for (int i = 0; i < stat.length; i++) {
	   if (stat[i].getPath().getName().startsWith("_"))
           	continue;
	   br=new BufferedReader(new InputStreamReader(fs.open(stat[i].getPath())));
           String line=br.readLine();
           while(line != null){
		PageRankNode node = new PageRankNode();	
		node.parseObject(line);
		nodes.add(node);
		line = br.readLine();
	   }
      }
    } catch (IOException e) {
        throw new RuntimeException("Error reading the file system!");
    }

    int numSources = nodes.get(0).getSources().size();

    for(int ind = 0; ind < numSources; ind++){
	final int i = ind;
	PriorityQueue<PageRankNode> queue = new PriorityQueue<PageRankNode>(n, new Comparator<PageRankNode>(){
	     public int compare(PageRankNode node1, PageRankNode node2){
		  float pr1 = node1.getPageRanks().get(i);
		  float pr2 = node2.getPageRanks().get(i);
		  if(pr1 < pr2) {
		      return 1;
		  } else if (pr1 > pr2) {
		      return -1;
		  }
		  return 0;
	     }
	});
	for(int j = 0; j < nodes.size(); j++){
	    queue.add(nodes.get(j));
	}

	Iterator<PageRankNode> it = queue.iterator();
	System.out.println("Source: " + nodes.get(0).getSources().get(i));
	int count = 0;
	while(it.hasNext() && count < n){
	   PageRankNode node = it.next();
	   String s = String.format("%.5f %d", Math.exp(node.getPageRanks().get(i)), node.getNodeId());
	   System.out.println(s);
	   count++;
	}
	System.out.println();
      }
      return 0;
  }

   public static void main(String[] args) throws Exception {
    	ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
   }
}
