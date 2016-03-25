/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudera.sa.securewordcount;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author vsingh
 */
public class SecureWordCountDriver extends Configured implements Tool {

  /**
   * @param args the command line arguments
   */
  public static void main(String[] args) {
    try {
      ToolRunner.run(new Configuration(), new SecureWordCountDriver(), args);
    } catch (Exception ex) {
      Logger.getLogger(SecureWordCountDriver.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration config = getConf();
    args = new GenericOptionsParser(config, args).getRemainingArgs();

    if (args.length < 2) {
     
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }
    Job job = Job.getInstance(config, this.getClass().getName() + "-wordcount");
    job.setJarByClass(SecureWordCountDriver.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true)?0:1;
    
  
  }
  
}
