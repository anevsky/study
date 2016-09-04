package mmgs.study.bigdata.hadoop.kw.appmaster;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import mmgs.study.bigdata.hadoop.kw.utils.HdfsManipulator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;

import static mmgs.study.bigdata.hadoop.kw.utils.KWConstants.*;

public class ApplicationMaster {

    private static final Logger LOG = Logger.getLogger(ApplicationMaster.class);

    private static final HdfsManipulator hdfsManipulator = HdfsManipulator.newInstance();

    public static void main(String[] args) throws Exception {
        String sourceDir = args[0];
        String targetDir = args[1];

        LOG.info("Start ApplicationMaster ...");
        // TODO: specify number of containers in property file
        final int numOfContainers = 2;
        Configuration conf = new YarnConfiguration();

        LOG.info("Initialize client to ResourceManager ...");
        AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
        rmClient.init(conf);
        rmClient.start();

        LOG.info("Initialize client to NodeManagers ...");
        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();

        LOG.info("Register ApplicationMaster in ResourceManager ...");
        rmClient.registerApplicationMaster(NetUtils.getHostname(), 0, "");

        LOG.info("Specifying priority for worker containers ...");
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        LOG.info("Setting Resource capability for Containers ...");
        Resource capability = Records.newRecord(Resource.class);
        // TODO: specify memory and vcores information in property file
        capability.setMemory(128);
        capability.setVirtualCores(1);
        for (int i = 0; i < numOfContainers; ++i) {
            ContainerRequest containerRequested = new ContainerRequest(capability, null, null, priority, true);
            // Resource, nodes, racks, priority and relax locality flag
            rmClient.addContainerRequest(containerRequested);
        }

        LOG.info("Adding Extractor application jar to local resources ...");
        Map<String, LocalResource> localResources = new HashMap<>();
        final Path containerJarPath = new Path(hdfsManipulator.getFS() + "/" + APPLICATION_FULL_PATH);
        LocalResource containerJar = Records.newRecord(LocalResource.class);
        FileStatus containerJarStat = FileSystem.get(conf).getFileStatus(containerJarPath);
        containerJar.setResource(ConverterUtils.getYarnUrlFromPath(containerJarPath));
        containerJar.setSize(containerJarStat.getLen());
        containerJar.setTimestamp(containerJarStat.getModificationTime());
        containerJar.setType(LocalResourceType.FILE);
        containerJar.setVisibility(LocalResourceVisibility.PUBLIC);
        localResources.put(APPLICATION_JAR, containerJar);

        LOG.info("Setting environment");
        Map<String, String> containerEnv = new HashMap<>();
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(containerEnv, ApplicationConstants.Environment.CLASSPATH.name(), c.trim());
        }
        Apps.addToEnvironment(containerEnv, ApplicationConstants.Environment.CLASSPATH.name(), ApplicationConstants.Environment.PWD.$() + File.separator + "*");

        // Point #6
        int allocatedContainers = 0;
        LOG.info("Requesting container allocation from ResourceManager");
        while (allocatedContainers < numOfContainers) {
            AllocateResponse response = rmClient.allocate(0);
            for (Container container : response.getAllocatedContainers()) {
                ++allocatedContainers;

                // Launch container by creating ContainerLaunchContext
                ContainerLaunchContext containerCtx = Records.newRecord(ContainerLaunchContext.class);
                containerCtx.setCommands(Collections.singletonList("java" + " -Xmx256M" + " " + " " + CONTAINER_MAIN_CLASS
                        + " " + sourceDir + " " + targetDir
                        + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                        + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));
                System.out.println("Starting container on node : " + container.getNodeHttpAddress());
                containerCtx.setEnvironment(containerEnv);
                containerCtx.setLocalResources(localResources);
                nmClient.startContainer(container, containerCtx);
            }
            Thread.sleep(100);
        }

        // Point #6
        int completedContainers = 0;
        while (completedContainers < numOfContainers) {
            AllocateResponse response = rmClient.allocate(completedContainers / numOfContainers);
            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                ++completedContainers;
                System.out.println("Container completed : " + status.getContainerId());
                System.out.println("Completed container " + completedContainers);
            }
            Thread.sleep(100);
        }
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
    }
}
