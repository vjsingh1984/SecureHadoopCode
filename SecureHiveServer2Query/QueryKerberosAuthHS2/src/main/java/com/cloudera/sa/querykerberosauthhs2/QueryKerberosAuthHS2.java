/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudera.sa.querykerberosauthhs2;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.hadoop.security.UserGroupInformation;

/**
 *
 * @author vsingh
 */
public class QueryKerberosAuthHS2 {

  /* This method signature allow you to kerberos authentication enabled 
  HiveServer2 instance 
  */
  
  public QueryKerberosAuthHS2() throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      
        UserGroupInformation.loginUserFromKeytab(
            "vijay@US-WEST-2.COMPUTE.INTERNAL","/etc/vijay.keytab");
      
      
    }
  }
  public Connection connect(String hostName, int port, String database, 
    String domain) throws ClassNotFoundException, SQLException {
    String hiveJDBCClassName = "org.apache.hive.jdbc.HiveDriver";
    String jdbcUrl = "jdbc:hive2://" + hostName + ":" + port + "/" + database +
        ";principal=hive/_HOST@"+domain;
    
    Class.forName(hiveJDBCClassName);
    Connection connection;
    connection = DriverManager.getConnection(jdbcUrl);
    return connection;
  }
  //Additionally one can add ssl=true with truststore location and password 
  // for kerberos + SSL/TSL connection;
  public Connection connect(String hostName, int port, String database, 
    String domain, boolean ssl) throws ClassNotFoundException, SQLException {
    String hiveJDBCClassName = "org.apache.hive.jdbc.HiveDriver";
    String jdbcUrl = "jdbc:hive2://" + hostName + ":" + port + "/" + database +
        ";principal=hive/_HOST@"+domain +";ssl="+ssl;
    if(ssl) {
        jdbcUrl += ";sslTrustStore=/opt/cloudera/security/jks/truststore/jssecacerts;trustStorePassword=changeit";
    }
    Class.forName(hiveJDBCClassName);
    Connection connection;
    connection = DriverManager.getConnection(jdbcUrl);
    return connection;
  }
  
  /**
   * @param Query
   * @param conn
   * @return 
   * @throws java.sql.SQLException
   */
  public boolean executeQueryStatement(Connection conn, String Query) 
      throws SQLException {
    return conn.createStatement().execute(Query);
  }
  
  public boolean executeQueryPreparedStatement(Connection conn ,String Query) 
      throws SQLException {
    return conn.prepareStatement(Query).execute();
  }

  public static void main( String [] args) throws SQLException, IOException,
      ClassNotFoundException {
    QueryKerberosAuthHS2 instance;
    instance = new QueryKerberosAuthHS2();
    Connection connection = instance.connect(
        "ip-10-20-0-5.us-west-2.compute.internal", 10000,
            "default", "US-WEST-2.COMPUTE.INTERNAL");
    boolean b = instance.executeQueryStatement(connection,
        "SELECT Count(1) FROM flightdata_p ");
  }
  
  

}
