/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudera.sa.querykerberosauthhs2;

import java.sql.Connection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author vsingh
 */
public class QueryKerberosAuthHS2Test {
  
  public QueryKerberosAuthHS2Test() {
  }
  
  @BeforeClass
  public static void setUpClass() {
  }
  
  @AfterClass
  public static void tearDownClass() {
  }
  
  @Before
  public void setUp() {
  }
  
  @After
  public void tearDown() {
  }

  /**
   * Test of connect method, of class QueryKerberosAuthHS2.
   */
  @Test
  public void testConnect_5args() throws Exception {
    System.out.println("connect");
    String hostName = "ip-10-20-0-5.us-west-2.compute.internal";
    int port = 10000;
    String database = "default";
    String domain = "US-WEST-2.COMPUTE.INTERNAL";
    boolean ssl = false;
    QueryKerberosAuthHS2 instance = new QueryKerberosAuthHS2();
    Connection expResult = null;
    Connection result = instance.connect(hostName, port, database, domain, ssl);
    assertTrue(expResult != result);
    result.close();
  }

  /**
   * Test of executeQueryStatement method, of class QueryKerberosAuthHS2.
   */
  @Test
  public void testExecuteQueryStatement() throws Exception {
    System.out.println("executeQueryStatement");
   
    String Query = "select count(*) from flightdata_t";
    QueryKerberosAuthHS2 instance = new QueryKerberosAuthHS2();
     Connection conn = instance.connect("ip-10-20-0-5.us-west-2.compute.internal", 
         10000, "default", "US-WEST-2.COMPUTE.INTERNAL",false);
    boolean expResult = true;
    boolean result = instance.executeQueryStatement(conn, Query);
    assertEquals(expResult, result);
  }

  /**
   * Test of executeQueryPreparedStatement method, of class QueryKerberosAuthHS2.
   */
  @Test
  public void testExecuteQueryPreparedStatement() throws Exception {
    System.out.println("executeQueryPreparedStatement");
    String Query = "select count(*) from flightdata_t";
    QueryKerberosAuthHS2 instance = new QueryKerberosAuthHS2();
     Connection conn = instance.connect("ip-10-20-0-5.us-west-2.compute.internal", 
         10000, "default", "US-WEST-2.COMPUTE.INTERNAL",false);
    boolean expResult = true;
    boolean result = instance.executeQueryPreparedStatement(conn, Query);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
  }
  
}
