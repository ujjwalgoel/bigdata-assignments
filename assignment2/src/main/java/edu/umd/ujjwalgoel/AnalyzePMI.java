/*
 * Cloud9: A Hadoop toolkit for working with big data
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.ujjwalgoel;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.InputStreamReader;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.ToolRunner;

import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfWritables;

import com.google.common.collect.Iterators;

import edu.umd.cloud9.io.SequenceFileUtils;

public class AnalyzePMI {
  private static final String INPUT = "input";

  @SuppressWarnings({ "static-access" })
  public static void main(String[] args) {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(INPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(AnalyzePMI.class.getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(-1);
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    System.out.println("input path: " + inputPath);
    
    BufferedReader br = null;
    int countPairs = 0;
    
    List<PairOfWritables<PairOfStrings, FloatWritable>> pmis = new ArrayList<PairOfWritables<PairOfStrings, FloatWritable>>();
    List<PairOfWritables<PairOfStrings, FloatWritable>> cloudPmis = new ArrayList<PairOfWritables<PairOfStrings, FloatWritable>>();
    List<PairOfWritables<PairOfStrings, FloatWritable>> lovePmis = new ArrayList<PairOfWritables<PairOfStrings, FloatWritable>>();


    PairOfWritables<PairOfStrings, FloatWritable> highestPMI = null;
    PairOfWritables<PairOfStrings, FloatWritable> highestCloudPMI = null;
    PairOfWritables<PairOfStrings, FloatWritable> highestCloudPMI2 = null;
    PairOfWritables<PairOfStrings, FloatWritable> highestCloudPMI3 = null;

    PairOfWritables<PairOfStrings, FloatWritable> highestLovePMI =  null;
    PairOfWritables<PairOfStrings, FloatWritable> highestLovePMI2 = null;
    PairOfWritables<PairOfStrings, FloatWritable> highestLovePMI3 = null;


    try {
    FileSystem fs = FileSystem.get(new Configuration());
    FileStatus[] status = fs.listStatus(new Path(inputPath));
    //PairOfStrings pair = new PairOfStrings();
    for(int i=0; i < status.length;i++){
	br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
	String line=br.readLine();
	while(line != null){
	    String[] words = line.split("\\t");
	    float value = Float.parseFloat(words[1].trim());
	    String[] wordPair = words[0].replaceAll("\\(", "").replaceAll("\\)", "").split(",");
	    PairOfStrings pair = new PairOfStrings();
	    pair.set(wordPair[0].trim(), wordPair[1].trim());
	    if(wordPair[0].trim().equals("cloud")){
		PairOfWritables<PairOfStrings, FloatWritable> cloudPmi = new PairOfWritables<PairOfStrings, FloatWritable>();
            	cloudPmi.set(pair,new FloatWritable(value));
            	cloudPmis.add(cloudPmi);
		if((highestCloudPMI == null) || (highestCloudPMI.getRightElement().compareTo(cloudPmi.getRightElement()) < 0)) {
                    highestCloudPMI = cloudPmi;
                } else if ((highestCloudPMI2 == null) || (highestCloudPMI2.getRightElement().compareTo(cloudPmi.getRightElement()) < 0)){
		    highestCloudPMI2 = cloudPmi;
		} else if ((highestCloudPMI3 == null) || (highestCloudPMI3.getRightElement().compareTo(cloudPmi.getRightElement()) < 0)){
                    highestCloudPMI3 = cloudPmi;
                }
	    }
	    if(wordPair[0].trim().equals("love")){
                PairOfWritables<PairOfStrings, FloatWritable> lovePmi = new PairOfWritables<PairOfStrings, FloatWritable>();
                lovePmi.set(pair,new FloatWritable(value));
                lovePmis.add(lovePmi);
		if((highestLovePMI == null) || (highestLovePMI.getRightElement().compareTo(lovePmi.getRightElement()) < 0)) {
                     highestLovePMI = lovePmi;
                 } else if ((highestLovePMI2 == null) || (highestLovePMI2.getRightElement().compareTo(lovePmi.getRightElement()) < 0)){
                     highestLovePMI2 = lovePmi;
                 } else if ((highestLovePMI3 == null) || (highestLovePMI3.getRightElement().compareTo(lovePmi.getRightElement()) < 0)){
                     highestLovePMI3 = lovePmi;
                 }
            }
	    PairOfWritables<PairOfStrings, FloatWritable> pmi = new PairOfWritables<PairOfStrings, FloatWritable>();
 	    pmi.set(pair,new FloatWritable(value));
	    pmis.add(pmi);
	    if(highestPMI == null){
		highestPMI = pmi;
 	    }
	    else if(highestPMI.getRightElement().compareTo(pmi.getRightElement()) < 0) {
		highestPMI = pmi;
	    }
	    countPairs++;
	    line=br.readLine();
	}
    }
   } catch(Exception ex){
	System.out.println("ERROR" + ex.getMessage());
   }

	
    /*Collections.sort(pmis, new Comparator<PairOfWritables<PairOfStrings, FloatWritable>>() {
      public int compare(PairOfWritables<PairOfStrings, FloatWritable> e1,
          PairOfWritables<PairOfStrings, FloatWritable> e2) {
        /*if (e2.getRightElement().compareTo(e1.getRightElement()) == 0) {
          return e1.getLeftElement().getLeftElement().compareTo(e2.getLeftElement().getLeftElement());
        }

        return e2.getRightElement().compareTo(e1.getRightElement());
      }
    });

    
    Collections.sort(cloudPmis, new Comparator<PairOfWritables<PairOfStrings, FloatWritable>>() {
      public int compare(PairOfWritables<PairOfStrings, FloatWritable> e1,
          PairOfWritables<PairOfStrings, FloatWritable> e2) {
        if (e2.getRightElement().compareTo(e1.getRightElement()) == 0) {
            return e1.getLeftElement().getLeftElement().compareTo(e2.getLeftElement().getLeftElement());
                    }

        return e2.getRightElement().compareTo(e1.getRightElement());
      }
    });


    Collections.sort(lovePmis, new Comparator<PairOfWritables<PairOfStrings, FloatWritable>>() {
      public int compare(PairOfWritables<PairOfStrings, FloatWritable> e1,
          PairOfWritables<PairOfStrings, FloatWritable> e2) {
        if (e2.getRightElement().compareTo(e1.getRightElement()) == 0) {
            return e1.getLeftElement().getLeftElement().compareTo(e2.getLeftElement().getLeftElement());
                   }

        return e2.getRightElement().compareTo(e1.getRightElement());
      }
    });

     PairOfWritables<PairOfStrings, FloatWritable> highestPMI = pmis.get(0);
     PairOfWritables<PairOfStrings, FloatWritable> highestCloudPMI = cloudPmis.get(0);	   PairOfWritables<PairOfStrings, FloatWritable> highestCloudPMI2 = cloudPmis.get(1);
     PairOfWritables<PairOfStrings, FloatWritable> highestCloudPMI3 = cloudPmis.get(2);
     
     PairOfWritables<PairOfStrings, FloatWritable> highestLovePMI = lovePmis.get(0);       PairOfWritables<PairOfStrings, FloatWritable> highestLovePMI2 = lovePmis.get(1);
     PairOfWritables<PairOfStrings, FloatWritable> highestLovePMI3 = lovePmis.get(2);*/

     System.out.println("Total Distinct Pairs : " + countPairs);
     System.out.println("Pair with highest PMI : (" + highestPMI.getLeftElement().getLeftElement() + ", " + highestPMI.getLeftElement().getRightElement());
     
     System.out.println("Word with highest PMI with Cloud : " + highestCloudPMI.getLeftElement().getRightElement() + " with value : " + highestCloudPMI.getRightElement().get());
    System.out.println("Word with second highest PMI with Cloud : " + highestCloudPMI2.getLeftElement().getRightElement() + " with value : " + highestCloudPMI2.getRightElement().get());
    System.out.println("Word with third highest PMI with Cloud : " + highestCloudPMI3.getLeftElement().getRightElement() + " with value : " + highestCloudPMI3.getRightElement().get());

    System.out.println("Word with highest PMI with Love : " + highestLovePMI.getLeftElement().getRightElement() + " with value : " + highestLovePMI.getRightElement().get());
    System.out.println("Word with second highest PMI with Love : " + highestLovePMI2.getLeftElement().getRightElement() + " with value : " + highestLovePMI2.getRightElement().get());
    System.out.println("Word with third highest PMI with Love : " + highestLovePMI3.getLeftElement().getRightElement() + " with value : " + highestLovePMI3.getRightElement().get());

  }
}
