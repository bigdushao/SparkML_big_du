package org.du.yarn;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by dushao on 18-1-22.
 */
public class JBossClient {
    private static final Logger LOG = Logger.getLogger(JBossClient.class.getName());

    private Configuration conf;
    private YarnClient yarnClient;
    private String appName = "";
    private int amPriority = 0; // applicationMaster 的优先级设置
    private String amQueue = "";
    private int amMemory = 1024;
    private String appJar = "";
    private final String appMasterMainClass = JBossApplicationMaster.class.getName();

    private int shellCmdPriority = 0; // shell 提交命令的优先级设置
    private int containerMemeory = 1024; // 设置Container的内存
    private int numContainers = 2; // 设置Container的数量
    // 权限设置
    private String adminUser;
    private String adminPassword;

    private String jbossAppUri; //
    private String log4jPropFile = "";
    boolean debugFlag = false;
    private Options opts; // 配置客户端命令行
    /**
     * @param args command line arguments
     * */
    public static void main(String[] args){
    boolean result = false;
    try {
        JBossClient client = new JBossClient();
        LOG.info("Initializing JBossClient");
        try{
            boolean doRun = client.init(args);
            if(!doRun){
                System.exit(0);
            }
        }catch (IllegalArgumentException e){
            System.err.println(e.getLocalizedMessage());
            client.printUsage();
            System.exit(-1);
        }
        result = client.run();
    }catch (Throwable t){
        LOG.log(Level.SEVERE, "ERROR running JBoss Client", t);
        System.exit(-1);
    }
    if(result){
        LOG.info("application completed successfully");
        System.exit(0);
    }
    LOG.log(Level.SEVERE, "Application failed to complete successfully");
    System.exit(2);
    }

    /**
     * 命令行的一些配置
     * */

    public  JBossClient(Configuration conf) throws Exception{
        this.conf = conf;
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        opts = new Options();
        opts.addOption("appname", true,
                "Application Name. Default value - JBoss on YARN");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("queue", true,
                "RM Queue in which this application is to be submitted");
        opts.addOption("timeout", true, "Application timeout in milliseconds");
        opts.addOption("master_memory", true,
                "Amount of memory in MB to be requested to run the application master");
        opts.addOption("jar", true,
                "JAR file containing the application master");
        opts.addOption("container_memory", true,
                "Amount of memory in MB to be requested to run the shell command");
        opts.addOption("num_containers", true,
                "No. of containers on which the shell command needs to be executed");
        opts.addOption("admin_user", true,
                "User id for initial administrator user");
        opts.addOption("admin_password", true,
                "Password for initial administrator user");
        opts.addOption("log_properties", true, "log4j.properties file");
        opts.addOption("debug", false, "Dump out debug information");
        opts.addOption("help", false, "Print usage");
    }

    public JBossClient() throws Exception{
        this(new YarnConfiguration());
    }

    /**
     * Helper function to print out usage
     * */

    private void printUsage() {
        new HelpFormatter().printHelp("Client", opts);
    }

    /**
     * parse command line options
     * @param args parsed command line options
     * @return whether the init was successful to run the client
     * @throws ParseException
     * */
    public boolean init(String[] args) throws ParseException{
        CommandLine cliparser = new GnuParser().parse(opts, args);
        /**
         * if(args.length == 0) {
         *     throw new
         *     IllegalArgumentException("No args specified for client to initialize");
         * }
         * */

        if(cliparser.hasOption("help")){
            printUsage();
            return false;
        }

        if(cliparser.hasOption("debug")){
            debugFlag = true;
        }

        appName = cliparser.getOptionValue("appname", "JBoss on Yarn");
        amPriority = Integer.parseInt(cliparser.getOptionValue("priority", "0"));
        amQueue = cliparser.getOptionValue("queue", "default");
        amMemory = Integer.parseInt(cliparser.getOptionValue("master_memory", "10"));
        if(amMemory < 0){
            throw  new IllegalArgumentException(
                    "Invalid memory specified for application master, exiting."
                    + "specified memory = " + amMemory
            );

        }

        if(!cliparser.hasOption("jar")){
            throw new IllegalArgumentException(
                    "No jar file specified for application master");
        }

        appJar = cliparser.getOptionValue("jar");
        containerMemeory = Integer.parseInt(cliparser.getOptionValue("container_memory", "10"));
        numContainers = Integer.parseInt(cliparser.getOptionValue("num_containers", "1"));
        adminUser = cliparser.getOptionValue("admin_user", "yarn");
        adminPassword = cliparser.getOptionValue("admin_password", "yarn");
        if(containerMemeory < 0 || numContainers < 1){
            throw  new IllegalArgumentException(
                    "Invalid no. of containers or container memory specified, exiting."
                            + " Specified containerMemory=" + containerMemeory
                            + ", numContainer=" + numContainers
            );
        }
        log4jPropFile = cliparser.getOptionValue("log_properties", "");
        return true;
    }

    /**
     * main run function for the client
     *
     * */

    public boolean run() throws IOException, YarnException{

        LOG.info("Running Client");
        yarnClient.start();

        YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
        LOG.info("Got cluster metric info form ASM , numNodeManagers = " + clusterMetrics.getNumNodeManagers());

        List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
        LOG.info("Got Cluster node info form ASM");
        for (NodeReport node : clusterNodeReports) {
            LOG.info("Got node report from ASM for" + ", nodeId="
                    + node.getNodeId() + ", nodeAddress"
                    + node.getHttpAddress() + ", nodeRackName"
                    + node.getRackName() + ", nodeNumContainers"
                    + node.getNumContainers());
        }

        QueueInfo queueInfo = yarnClient.getQueueInfo(this.amQueue);
        LOG.info("Queue info" + ", queueName=" + queueInfo.getQueueName()
                + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
                + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
                + ", queueApplicationCount="
                + queueInfo.getApplications().size()
                + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

        List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
        for (QueueUserACLInfo aclInfo : listAclInfo) {
            for (QueueACL userAcl : aclInfo.getUserAcls()) {
                LOG.info("User ACL Info for Queue" + ", queueName="
                        + aclInfo.getQueueName() + ", userAcl="
                        + userAcl.name());
            }
        }

        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        int maxMem = appResponse.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

        if (amMemory > maxMem) {
            LOG.info("AM memory specified above max threshold of cluster. Using max value."
                    + ", specified=" + amMemory + ", max=" + maxMem);
            amMemory = maxMem;
        }

        ApplicationSubmissionContext appContext = app
                .getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        appContext.setApplicationName(appName);

        ContainerLaunchContext amContainer = Records
                .newRecord(ContainerLaunchContext.class);

        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        LOG.info("Copy App Master jar from local filesystem and add to local environment");
        FileSystem fs = FileSystem.get(conf);
        Path src = new Path(appJar);
        String pathSuffix = appName + File.separator + appId.getId()
                + File.separator
                + JBossConstants.JBOSS_ON_YARN_APP;
        Path dst = new Path(fs.getHomeDirectory(), pathSuffix);
        jbossAppUri = dst.toUri().toString();
        fs.copyFromLocalFile(false, true, src, dst);
        FileStatus destStatus = fs.getFileStatus(dst);
        LocalResource amJarRsrc = Records.newRecord(LocalResource.class);

        amJarRsrc.setType(LocalResourceType.FILE);
        amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
        amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(dst));
        amJarRsrc.setTimestamp(destStatus.getModificationTime());
        amJarRsrc.setSize(destStatus.getLen());
        localResources.put(JBossConstants.JBOSS_ON_YARN_APP,
                amJarRsrc);

        // 配置日志文件
        if (!StringUtils.isEmpty(log4jPropFile)) {
            Path log4jSrc = new Path(log4jPropFile); // 生成日志的地址属性
            Path log4jDst = new Path(fs.getHomeDirectory(), "log4j.props"); // 日志保存的位置
            fs.copyFromLocalFile(false, true, log4jSrc, log4jDst); // 将日志从本地文件Copy到hdfs上

            FileStatus log4jFileStatus = fs.getFileStatus(log4jDst);
            LocalResource log4jRsrc = Records.newRecord(LocalResource.class);
            log4jRsrc.setType(LocalResourceType.FILE);
            log4jRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
            log4jRsrc.setResource(ConverterUtils.getYarnUrlFromURI(log4jDst
                    .toUri()));
            log4jRsrc.setTimestamp(log4jFileStatus.getModificationTime());
            log4jRsrc.setSize(log4jFileStatus.getLen());
            localResources.put("log4j.properties", log4jRsrc);
        }
        // 文件本地化
        amContainer.setLocalResources(localResources);

        LOG.info("Set the environment for the application master");
        Map<String, String> env = new HashMap<String, String>();

        StringBuilder classPathEnv = new StringBuilder(
                Environment.CLASSPATH.$()).append(File.pathSeparatorChar)
                .append("./*");

        // 配置yarn的环境变量
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            classPathEnv.append(File.pathSeparatorChar);
            classPathEnv.append(c.trim());
        }

        classPathEnv.append(File.pathSeparatorChar)
                .append("./log4j.properties");

        if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
            classPathEnv.append(':');
            classPathEnv.append(System.getProperty("java.class.path"));
        }

        env.put("CLASSPATH", classPathEnv.toString());

        amContainer.setEnvironment(env);

        Vector<CharSequence> vargs = new Vector<CharSequence>(30);

        LOG.info("Setting up app master command");
        vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
        vargs.add("-Xmx" + amMemory + "m");
        vargs.add(appMasterMainClass);
        vargs.add("--container_memory " + String.valueOf(containerMemeory));
        vargs.add("--num_containers " + String.valueOf(numContainers));
        vargs.add("--priority " + String.valueOf(shellCmdPriority));
        vargs.add("--admin_user " + adminUser);
        vargs.add("--admin_password " + adminPassword);
        vargs.add("--jar " + jbossAppUri);

        if (debugFlag) {
            vargs.add("--debug");
        }

        vargs.add("1>" + JBossConstants.JBOSS_CONTAINER_LOG_DIR
                + "/JBossApplicationMaster.stdout");
        vargs.add("2>" + JBossConstants.JBOSS_CONTAINER_LOG_DIR
                + "/JBossApplicationMaster.stderr");

        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        LOG.info("Completed setting up app master command "
                + command.toString());
        List<String> commands = new ArrayList<String>();
        commands.add(command.toString());
        amContainer.setCommands(commands);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(amMemory);
        appContext.setResource(capability);

        appContext.setAMContainerSpec(amContainer);

        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(amPriority);
        appContext.setPriority(pri);

        appContext.setQueue(amQueue);

        LOG.info("Submitting the application to ASM");

        yarnClient.submitApplication(appContext);

        return monitorApplication(appId);
    }

    /**
     * Monitor the submitted application for completion .kill application if time expires
     * @param appId
     *          application id of application to be monitored
     * @throws YarnException
     * @throws IOException
     * */

    private boolean monitorApplication(ApplicationId appId) throws YarnException, IOException{
        while (true){
            try {
                Thread.sleep(1000);
            }catch (InterruptedException e){
                LOG.finest(" Thread sleep in monitoring loop interrupted");
            }
            // 通过AppId即可获取任务运行的状态
            ApplicationReport report = yarnClient.getApplicationReport(appId);

            LOG.info("Got application report from ASM for" + ", appId="
                    + appId.getId() + ", clientToAMToken="
                    + report.getClientToAMToken() + ", appDiagnostics="
                    + report.getDiagnostics() + ", appMasterHost="
                    + report.getHost() + ", appQueue=" + report.getQueue()
                    + ", appMasterRpcPort=" + report.getRpcPort()
                    + ", appStartTime=" + report.getStartTime()
                    + ", yarnAppState="
                    + report.getYarnApplicationState().toString()
                    + ", distributedFinalState="
                    + report.getFinalApplicationStatus().toString()
                    + ", appTrackingUrl=" + report.getTrackingUrl()
                    + ", appUser=" + report.getUser());
            // yarn 提交的任务的状态
            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus jbossStatus = report.getFinalApplicationStatus();

            if(YarnApplicationState.FINISHED == state){
                if(FinalApplicationStatus.SUCCEEDED == jbossStatus){
                    LOG.info("application has completed successfully. Breaking monitoring loop");
                    return true;
                } else {
                    LOG.info("application did finished nusuccessfully." +
                    "YarnState = " + state.toString() +
                    ", JBASFinalStatus = " + jbossStatus.toString() +
                    ", breaking monitoring loop");
                    return false;
                }
            }else if(YarnApplicationState.KILLED == state || YarnApplicationState.FAILED == state){
                LOG.info("Application did not finish." + " YarnState="
                        + state.toString() + ", JBASFinalStatus="
                        + jbossStatus.toString() + ". Breaking monitoring loop");
                return false;

            }
        }
    }
}