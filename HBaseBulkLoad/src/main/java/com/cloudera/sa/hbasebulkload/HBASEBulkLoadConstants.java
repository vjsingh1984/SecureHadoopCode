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
public class HBASEBulkLoadConstants {
  public static final String HBASE_TABLE_KEY = "hbase.table.name";
  public static final String HBASE_COLUMNS_KEY = "hbase.table.column.names";
  public static final String HBASE_COLUMN_FAMILY_KEY = 
      "hbase.table.column.family.name";
  public static final String HBASE_COLUMN_SEPERATOR_KEY = 
      "hbase.table.column.seperator";
  public static final int SUCCESS = 0;
  public static final int FAILURE = 1;
  
}
