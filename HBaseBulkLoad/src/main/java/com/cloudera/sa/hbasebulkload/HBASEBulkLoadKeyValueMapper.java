/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudera.sa.hbasebulkload;

import au.com.bytecode.opencsv.CSVParser;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author vsingh
 */
class HBASEBulkLoadKeyValueMapper extends
    Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

  CSVParser csvParser = null;
  String hbaseTabName = "";
  String hbaseColumnFamily = "";
  String[] hbaseColumns = {""};
  ImmutableBytesWritable hKey = new ImmutableBytesWritable();
  Put hPut;
  String hbaseColumnsSeperator;
  String[] outputFields;
  int noOfColumns = 0;

  @Override
  protected void setup(Context context) throws IOException,
      InterruptedException {
    Configuration config = context.getConfiguration();
    hbaseTabName = config.get(HBASEBulkLoadConstants.HBASE_TABLE_KEY);
    hbaseColumnFamily = config.get(HBASEBulkLoadConstants.HBASE_COLUMN_FAMILY_KEY);

    hbaseColumnsSeperator = config.get(
        HBASEBulkLoadConstants.HBASE_COLUMN_SEPERATOR_KEY);
    csvParser = new CSVParser(hbaseColumnsSeperator.charAt(0));
    System.out.println(5);
    hbaseColumns = csvParser.parseLine(config.get(
        HBASEBulkLoadConstants.HBASE_COLUMNS_KEY));
    noOfColumns = hbaseColumns.length;

  }

  /*@Override
   protected void cleanup(Context context) {
   }*/
  @Override
  protected void map(LongWritable key, Text line, Context context)
      throws IOException, InterruptedException {

    outputFields = csvParser.parseLine(line.toString());
    hKey.set(outputFields[0].getBytes());
    hPut = new Put(outputFields[0].getBytes());
    for (int i = 1; i < noOfColumns; i++) {
      if (hbaseColumns.length > i && 
          outputFields.length > i && 
          outputFields[i] != null &&
          !outputFields[i].trim().isEmpty()) {
         hPut = hPut.addColumn(hbaseColumnFamily.getBytes(), 
             hbaseColumns[i].getBytes(),
            outputFields[i].getBytes());
      }

    }
    context.write(hKey, hPut);
  }
}
