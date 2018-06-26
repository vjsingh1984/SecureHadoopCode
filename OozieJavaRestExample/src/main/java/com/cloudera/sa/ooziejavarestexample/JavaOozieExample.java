/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudera.sa.ooziejavarestexample;

/**
 *
 * @author vijaykumarsingh
 */
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.AuthOozieClient;

import java.io.FileInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.text.MessageFormat;
import org.apache.oozie.client.OozieClientException;

public class JavaOozieExample {

    public static void main(String... args) {
        System.exit(execute(args));
    }

    static int execute(String... args) {
        if (args.length != 2) {
            System.out.println();
            System.out.println("Expected parameters: <WF_APP_HDFS_URI> <WF_PROPERTIES>");
            return -1;
        }
        String appUri = args[0];
        String propertiesFile = args[1];
        if (propertiesFile != null) {
            File file = new File(propertiesFile);
            if (!file.exists()) {
                System.out.println();
                System.out.println("Specified Properties file does not exist: " + propertiesFile);
                return -1;
            }
            if (!file.isFile()) {
                System.out.println();
                System.out.println("Specified Properties file is not a file: " + propertiesFile);
                return -1;
            }
        }

        try {
            // get a OozieClient for local Oozie
            OozieClient wc = new AuthOozieClient("https://ooziehost.fqdn:11443/oozie");
            // create a workflow job configuration and set the workflow application path
            Properties conf = wc.createConfiguration();
            conf.setProperty(OozieClient.APP_PATH, appUri + File.separator + "workflow.xml");
            conf.setProperty("mapreduce.jobtracker.kerberos.principal", "mapred/_HOST@REALM");
            conf.setProperty("dfs.namenode.kerberos.principal", "hdfs/_HOST@REALM");
            // load additional workflow job parameters from properties file
            if (propertiesFile != null) {
                conf.load(new FileInputStream(propertiesFile));
            }

            String jobId = "myjobid";
            // wait until the workflow job finishes printing the status every 10 seconds
            while (wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
                System.out.println("Workflow job running ...");
                printWorkflowInfo(wc.getJobInfo(jobId));
                Thread.sleep(10 * 1000);
            }

            // print the final status o the workflow job
            System.out.println("Workflow job completed ...");
            printWorkflowInfo(wc.getJobInfo(jobId));
            return (wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.SUCCEEDED) ? 0 : -1;
        } catch (IOException | OozieClientException | InterruptedException ex) {
            System.out.println();
            System.out.println(ex.getMessage());
            return -1;
        }
    }

    private static void printWorkflowInfo(WorkflowJob wf) {
        System.out.println("Application Path   : " + wf.getAppPath());
        System.out.println("Application Name   : " + wf.getAppName());
        System.out.println("Application Status : " + wf.getStatus());
        System.out.println("Application Actions:");
        wf.getActions().stream().forEach((action) -> {
            System.out.println(MessageFormat.format("   Name: {0} Type: {1} Status: {2}", action.getName(),
                action.getType(), action.getStatus()));
        });
        System.out.println();
    }

}
