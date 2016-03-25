/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudera.sa.securewordcount;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.mortbay.log.Log;

/**
 *
 * @author vsingh
 */
class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private  Configuration conf;
  private  StringTokenizer strTokenizer;
  private  Text word = new Text();
  private final IntWritable one = new IntWritable(1);
  private static final String tokenDelim = "\\s";
  Counter tokenizerMapperLines;
  @Override
  protected void setup(Context context) {
    conf = context.getConfiguration();
    
    tokenizerMapperLines = context.getCounter("TokenizerMapperLines", "TokenizerMapperProcessed");
    
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    strTokenizer = new StringTokenizer(value.toString(),tokenDelim,true);
    while(strTokenizer.hasMoreElements()) {
      word.set(strTokenizer.nextToken());
      context.write(word, one);
    }
    tokenizerMapperLines.increment(1);
  }

  @Override
  protected void cleanup(Context context) {
    Log.info("Counter for lines: " + tokenizerMapperLines.getValue());
  }

}
