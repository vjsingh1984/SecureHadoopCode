/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudera.sa.hbasebulkload;

/**
 *
 * @author vsingh
 */
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutCombiner;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HBASEBulkLoadDriver extends Configured implements Tool {

  /**
   * @param args the command line arguments
   * @throws java.lang.Exception
   */
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(),
        new HBASEBulkLoadDriver(), args);

  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration config = getConf();
    args = new GenericOptionsParser(config, args).getRemainingArgs();

    if (args.length < 6) {
      /*System.out.println("hadoop jar HBASEBulkLoad.jar "
       + "com.cloudera.sa.hbasebulkload.HBASEBulkLoadDriver"
       + " <inputpath> <outputpath> <hbaseTable> <hbaseColumnFamily"
       + " \"<hbaseColumns (delimiter seperated)>\" <column delimiter>");*/
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }

    String hbaseTab = args[2];
    String hbaseColumnFamily = args[3];
    String hbaseColumns = args[4];
    String hbaseColumnSeperator = args[5];
    config.set(HBASEBulkLoadConstants.HBASE_TABLE_KEY,
        hbaseTab.trim().toLowerCase(Locale.ENGLISH));
    config.set(HBASEBulkLoadConstants.HBASE_COLUMN_FAMILY_KEY,
        hbaseColumnFamily);
    config.set(HBASEBulkLoadConstants.HBASE_COLUMNS_KEY,
        hbaseColumns.trim().toLowerCase(Locale.ENGLISH));
    config.set(HBASEBulkLoadConstants.HBASE_COLUMN_SEPERATOR_KEY,
        hbaseColumnSeperator);
    System.out.println(2);
    Job job = Job.getInstance(config, this.getClass().getName() + "-" + hbaseTab);
    HBaseConfiguration.addHbaseResources(config);

    job.setInputFormatClass(TextInputFormat.class);
    job.setJarByClass(HBASEBulkLoadDriver.class);
    job.setMapperClass(HBASEBulkLoadKeyValueMapper.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Put.class);
    job.setCombinerClass(PutCombiner.class);
    job.setReducerClass(PutSortReducer.class);

    Connection connection = ConnectionFactory.createConnection(config);
    Table hTab = connection.getTable(TableName.valueOf(hbaseTab));

    FileSystem.get(getConf()).delete(new Path(args[1]), true);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    //job.setOutputFormatClass(HFileOutputFormat2.class);
    TableMapReduceUtil.initTableReducerJob(hTab.getName().getNameAsString(),
        null, job);
    //job.setNumReduceTasks(0);
    TableMapReduceUtil.addDependencyJars(job);
    HFileOutputFormat2.configureIncrementalLoadMap(job, hTab);

    int exitCode = job.waitForCompletion(true)
        ? HBASEBulkLoadConstants.SUCCESS : HBASEBulkLoadConstants.FAILURE;
    System.out.println(8);
    if (HBASEBulkLoadConstants.SUCCESS == exitCode) {
      LoadIncrementalHFiles loader = new LoadIncrementalHFiles(config);
      loader.doBulkLoad(new Path(args[1]), (HTable)hTab);
      connection.close();
    }
    return exitCode;
  }

}
