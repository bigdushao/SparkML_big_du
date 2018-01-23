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
 *
 *
 */
public class JBossClient {
    private static final Logger LOG = Logger.getLogger(JBossClient.class.getName());
    // 定义一些全局变量 大部分全局变量会根据命令行参数进行初始化
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
        /*确保Yarn的环境对客户端可用。YarnConfiguration是Configuration的子类。创建YarnClient对象时，
        * 参数是YarnConfiguration对象。成功创建YarnConfiguration对象而不产生异常，要正确配置yarn-site.xml
        * 和yarn-default.xml这两个文件
        * 保证两个配置文件，能在classpath中找到它们，确保在classpath中没有重复和冲突的配置文件，会避免因此浪费不必要的时间。
        * yarn-site.xml一般在Hadoop的标准配置文件目录:～/etc/hadoop。如果一个配置项在yarn-site.xml中没有显示指定，这个配置项
        * 的默认值将由yarn-default.xml提供，yarn-default.xml在hadoop-yarn-common.jar里，
        * 这个jar包一般在Hadoop的默认的classpath中*/
        this.conf = conf;
        /*YarnConfiguration创建成功，通过YarnClient的工厂方法创建YarnClient对象
        * YarnClient是Yarn提供的一个服务，服务是Yarn的核心架构的一部分，日志聚集服务，节点健康状态的服务，
        * Shuffle处理器，都是Yarn的服务，服务都由管理自身声明周期的初始，开始和终止的这些方法。
        * 还有一些获取服务状态，失败时获取原因的方法，还可以向服务注册监听器，在某些特定事发生时，注册的回调函数将得到调用。*/

        /*YarnClient对象初始化时，首先从yarn-site.xml或者yarn-default.xml中的yarn.resource-manager.address属性获取ResourceManager
        * 的IP地址和端口。接着，从yarn.client.appsubmission.poll-interval属性获取轮询应用程序状态的周期，单位是毫秒，默认值是1000*/

        /*YarnClient对象初始化之后，会在内部创建一个ResourceManager客户端代理。Yarn application的开发者不需要直接跟这个代理打交道，
        * 调用YarnClient的方法就可以了.
        * YarnClient对象创建以后，通过Options来添加命令行参数。对每个命令行参数，需要3个参数传给Options.addOption方法。
        * (1)参数名，
        * (2)参数是否需要用户指定值
        * (3)参数的描述，当用户需要命令行帮助或者输入了错误的参数时，输出到终端上*/

        yarnClient = YarnClient.createYarnClient(); // 使用Crate方法实例化YarnClient
        yarnClient.init(conf); // 初始化yarn的配置，对输入参数进行解析

        /*
        * YarnClient对象创建以后，通过Options来添加命令行参数。对每个命令行参数，需要3个参数传给Options.addOption方法。
        * (1)参数名，
        * (2)参数是否需要用户指定值
        * (3)参数的描述，当用户需要命令行帮助或者输入了错误的参数时，输出到终端上
        * 应用程序的名字，优先级，queue，ApplicationMaster和Container所需的内存大小，大多数参数都是Yarn环境所需要的。
        * */
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
    /**
     * YarnClient的构造函数使用调用的是将YarnConfiguration作为参数的构造方法，
     * */
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
     * 对数输入的参数进行解析，首先实在YarnConfiguration初始化完成之后
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
     * 方法中包含YarnClient的创建，初始化和运行这些
     * YarnClient是一个Yarn服务，有管理声明周期的一些方法。
     * 通过YarnClient的对象可以获取很多集群相关的信息
     * */

    public boolean run() throws IOException, YarnException{

        LOG.info("Running Client");
        yarnClient.start();
        /*获取集群的信息，获取的集群信息,获取节点个数*/
        YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
        LOG.info("Got cluster metric info form ASM , numNodeManagers = " + clusterMetrics.getNumNodeManagers());
        /*获取NodeReport对象，查看集群各个节点的状态*/
        List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
        LOG.info("Got Cluster node info form ASM");
        for (NodeReport node : clusterNodeReports) {
            LOG.info("Got node report from ASM for" + ", nodeId="
                    + node.getNodeId() + ", nodeAddress"
                    + node.getHttpAddress() + ", nodeRackName"
                    + node.getRackName() + ", nodeNumContainers"
                    + node.getNumContainers());
        }
        /*获取集群中的任务队列*/
        QueueInfo queueInfo = yarnClient.getQueueInfo(this.amQueue);
        LOG.info("Queue info" + ", queueName=" + queueInfo.getQueueName()
                + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
                + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
                + ", queueApplicationCount="
                + queueInfo.getApplications().size()
                + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

        /*获取集群中的权限信息*/
        List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
        for (QueueUserACLInfo aclInfo : listAclInfo) {
            for (QueueACL userAcl : aclInfo.getUserAcls()) {
                LOG.info("User ACL Info for Queue" + ", queueName="
                        + aclInfo.getQueueName() + ", userAcl="
                        + userAcl.name());
            }
        }

        /*YarnClientApplication对象，通过这个对象来设置一些关键对象:GetNewApplicationResponse和ApplicationSubmissionContext
        * GetNewApplicationResponse可以得到集群的最大可用资源，如最大可用内存和最大可用虚拟CPU核数。
        * ApplicationSubmissionContext可以得到Yarn集群唯一的applicationID，另外，也是通过这个对象，设置ApplicationMaster的运行参数*/
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        int maxMem = appResponse.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

        if (amMemory > maxMem) {
            LOG.info("AM memory specified above max threshold of cluster. Using max value."
                    + ", specified=" + amMemory + ", max=" + maxMem);
            amMemory = maxMem;
        }

        /* ApplicationSubmissionContext可以得到Yarn集群唯一的applicationID，另外，也是通过这个对象，设置ApplicationMaster的运行参数*/
        ApplicationSubmissionContext appContext = app
                .getApplicationSubmissionContext();

        // applicationId
        ApplicationId appId = appContext.getApplicationId();
        // 设置ApplicationName
        appContext.setApplicationName(appName);
        /*设置给ApplicationSubmissionContext的值是ContainerLaunchContext，ApplicationMaster本事就是一个Yarn Container，
        * 创建一个org.apache.hadoop.yarn.api.records.ContainerLaunchContext对象，启动ApplicationMaster本身的这个Container
        * ContainerLaunchContext包含了ApplicationMaster运行时所需要的资源，最典型的是包含ApplicationMaster的class的Jar包，
        * 描述这些资源的是一个Map对象。Map的key和value分别是java.lang.String和org.apache.hadoop.yarn.api.records.LocalResource
        * key会被用来建立一个Container可见的本地文件系统的软链接*/
        ContainerLaunchContext amContainer = Records
                .newRecord(ContainerLaunchContext.class);

        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        LOG.info("Copy App Master jar from local filesystem and add to local environment");

        // 将本地的文件上传到Hdfs文件系统，其他节点的Container能够获取访问该资源，将该资源在进行本地化
        // 为了便于YARN进行验证，把修改时间和文件大小也写到LocalResources对象。然后把这个对象加入到LocalResource Map中
        // 再把Map加入ContainerLaunchContext对象。

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

        /*ApplicationMaster的Jar包对应的LocalResources对象定义为LocalResourceType.FILE类型，
        * 这样会被直接拷贝到Container的本地文件系统而不需奥解压*/
        amJarRsrc.setType(LocalResourceType.FILE);
        // 设置资源的可见性，APPLICATION意思是只有这个应用程序可以访问这个资源
        // 可见性还可以设置为所有application都可以访问，或者同一用户提交的应用程序都可以访问
        amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
        amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(dst));
        amJarRsrc.setTimestamp(destStatus.getModificationTime());
        amJarRsrc.setSize(destStatus.getLen());
        // 添加运行的Jar文件
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

        /*在ApplicationMaster的设置环境中，获取客户端的类路径，客户端的Yarn环境的类路径，并利用这些值构建类路径字符串。
        * 要设置的环境变量由java.lang.String的映射组成，其中添加了CLASSPATH中的关键字组成的类路径*/

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
        // 设置ApplicationMaster的ClassPath
        amContainer.setEnvironment(env);

        /* 构造启动ApplicationMaster的命令行。大多数命令行参数就是用户指定给client的，使用标准的Java命令
        * Yarn通过org.apache.hadoop.yarn.api.ApplicationConstants.Environment 提供构造方法
        * 示例中使用了Environment.JAVA_HOME.$(), 在Windows环境下，自动在环境变量名前后加上'%'符号，
        * 在命令后面重定向stdout和stderr到log文件。可以指定任意目录和文件，只要Container有写权限。
        * 实例中使用的是硬编码，更好的办法是YarnConfiguration对象的get('yarn.log.dir')来获取log文件路径*/

        // 构造启动ApplicationMaster启动命令，并把启动命令加入ContainerLaunchContext
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
        /* ContainerLaunchContext接受java.util.List对象，其中每个元素就是一条shell命令。Yarn 会将这些Shell命令组成一个Shell脚本，用来启动Container
        * 对于ApplicationMaster，只需要一条命令。所以，Java命令加到list之后。使用list来指定ContainerLaunchContext的启动命令*/

        // 将启动命令加入到ContainerLaunchContext中
        amContainer.setCommands(commands);

        /*指定应用程序的资源需求，Yarn应用程序最重要的一个资源是内存，如何通过标准方式定义内存资源需求，并把它加到ApplicationSubmissionContext中
        * 另一个重要的设置应用程序的队列和优先级，
        * Capacity调度器。调度器是可插拔的，可以在yarn-site.xml中设定，以便于管理运行多个并行作业的Yarn集群。*/
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(amMemory);
        appContext.setResource(capability);

        /*设置了ContainerLaunchContext的启动命令后，可以告诉ApplicationSubmissionContext，
        刚才准备的ContainerLaunchContext是针对ApplicationMaster，ApplicationMaster也是作为一个Container启动的，
        因为ApplicationMaster的Container只有一个，Yarn有专门有方法设置ContainerLaunchContext*/
        appContext.setAMContainerSpec(amContainer); // 专门设置ApplicationMaster的container启动

        // 设置优先级
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(amPriority);
        appContext.setPriority(pri);
        // 设置队列
        appContext.setQueue(amQueue);

        LOG.info("Submitting the application to ASM");

        yarnClient.submitApplication(appContext);
        // 通过YarnClient提交应用程序，在客户端代码中，提交应用程序的方法会阻塞，直到ResourceManager返回应用程序的状态为ACCEPTED
        return monitorApplication(appId);
    }

    /**
     * 通过YarnClient每获取一次应用程序的状态信息，这些状态信息封装在一个ApplicationReport对象中。ApplicationReport有一些便利的方法来获取应用
     * 的信息例如:应用的ID，重要的是应用的状态和终止情况。这些信息都会输出，直到应用成功完成或者被ResourceManager杀掉
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
