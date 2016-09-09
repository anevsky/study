package mmgs.study.bigdata.hadoop.kw.client;

import java.io.File;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import mmgs.study.bigdata.hadoop.kw.utils.HdfsManipulator;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;

import static mmgs.study.bigdata.hadoop.kw.utils.KWConstants.*;

public class Client {

    private static final Logger LOG = Logger.getLogger(Client.class);

    private static final HdfsManipulator hdfsManipulator = HdfsManipulator.newInstance();

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            printUsage(System.out);
            return;
        }
        // TODO: validate incoming arguments
        // TODO: check if directories exist and source directory is not empty
        String sourceDir = args[0];
        String targetDir = args[1];
        Integer numOfContainers = Integer.parseInt(args[2]);
        try {
            Client clientObj = new Client();
            clientObj.run(sourceDir, targetDir, numOfContainers);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run(String sourceDir, String targetDir, Integer numOfContainers) throws Exception {
        final Path appMasterJarPath = new Path(hdfsManipulator.getFS() + "/" + APPLICATION_FULL_PATH);

        // TODO: make proper init method
        LOG.info("Copying executables to HDFS ... ");
        hdfsManipulator.createHdfsDirectory(APPLICATION_DIRECTIRY);
        hdfsManipulator.copyLocalToHdfs(APPLICATION_JAR, APPLICATION_FULL_PATH);

        LOG.info("Initializing YARN configuration ... ");
        YarnConfiguration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        LOG.info("Requesting ResourceManager for a new Application ... ");
        YarnClientApplication app = yarnClient.createApplication();

        LOG.info("Initializing ContainerLaunchContext for ApplicationMaster container ");
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);


        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        LOG.info("Adding ApplicationMaster jar to local resources ... ");
        LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        FileStatus appMasterJarStat = FileSystem.get(conf).getFileStatus(appMasterJarPath);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(appMasterJarPath));
        appMasterJar.setSize(appMasterJarStat.getLen());
        appMasterJar.setTimestamp(appMasterJarStat.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
        localResources.put(APPLICATION_JAR, appMasterJar);

        amContainer.setLocalResources(Collections.unmodifiableMap(localResources));

        LOG.info("Setting environment");
        Map<String, String> appMasterEnv = new HashMap<>();
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), c.trim());
        }
        Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), Environment.PWD.$() + File.separator + "*");

        // TODO: set memory and vcores settings via property file
        LOG.info("Setting resource capability");
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(256);
        capability.setVirtualCores(1);

        LOG.info("Setting command to start ApplicationMaster service");
        // TODO: set JVM options in property file
        amContainer.setCommands(Collections.singletonList("java" + " -Xmx256M" + " " + APP_MASTER_MAIN_CLASS
                + " " + sourceDir + " " + targetDir + " " + numOfContainers
                + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));
        amContainer.setEnvironment(appMasterEnv);

        LOG.info("Initializing ApplicationSubmissionContext");
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appContext.setApplicationName(YARN_APPLICATION_NAME);
        appContext.setApplicationType("YARN");
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setQueue("default");
        ApplicationId appId = appContext.getApplicationId();

        LOG.info("Submitting application " + appId);
        yarnClient.submitApplication(appContext);
        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while (appState != YarnApplicationState.FINISHED
                && appState != YarnApplicationState.KILLED
                && appState != YarnApplicationState.FAILED) {
            Thread.sleep(100);
            appReport = yarnClient.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();
        }

        LOG.info("Application completed with status " + appState.toString());

        // TODO: make proper teardown method (cleanup all application directories and jars from hdfs)
    }

    private static void printUsage(PrintStream stream) {
        stream.println("Usage: " + APPLICATION_NAME + "<in_file> <out_dir> <numOfContainers>");
    }

}