package org.du.yarn;


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by dushao on 18-1-22.
 *
 * Client for JBoss Application Master submission to YARN
 *
 * 一个新创建的应用程序首先向ResourceManager注册自己，
 * 然否以Container的形式从ResourceManager请求资源，
 * 请求到资源以后跟NodeManager通信
 * 启动Container
 * 监控Container的运行
 */
public class JBossApplicationMaster {
    /*ApplicationMaster也依次执行构造函数、初始化和运行的流程，在构造函数里，先创建一个YarnConfiguration对象，
    * 这个对象包含了ApplicationMaster本身的Container对应的Yarn环境设置。
    * 构造函数里面ApplicationMaster本身的Container对应的Yarn环境设置。构造函数里面还定义了命令行参数，
    * 构造函数写完后，再添加一个初始化的方法*/
    private static final Logger LOG = Logger.getLogger(JBossApplicationMaster.class.getName());

    private Configuration conf;
    @SuppressWarnings("rawtypes")
    private AMRMClientAsync resourceManager;
    private NMClientAsync nmClientAsync;
    private NMCallbackHandler containerListener;

    private ApplicationAttemptId appAttemptID;

    private String appMasterHostname = "";
    private int appMasterRpcPort = 0;
    private String appMasterTrackingUrl = "";

    private int numTotalContainers = 2;
    private int containerMemory = 1024;
    private int requestPriority;

    private String adminUser;
    private String adminPassword;

    private AtomicInteger numCompletedContainers = new AtomicInteger();
    private AtomicInteger numAllocatedContainers = new AtomicInteger();
    private AtomicInteger numFailedContainers = new AtomicInteger();
    private AtomicInteger numRequestedContainers = new AtomicInteger();

    private Map<String, String> shellEnv = new HashMap<String, String>();

    private String jbossHome;
    private String appJar;
    private String domainController;

    private volatile boolean done;
    private volatile boolean success;

    private List<Thread> launchThreads = new ArrayList<Thread>();

    /**
     * @param args
     *            Command line args
     */
    public static void main(String[] args) {
        boolean result = false;
        try {
            JBossApplicationMaster appMaster = new JBossApplicationMaster();
            LOG.info("Initializing JBossApplicationMaster");
            boolean doRun = appMaster.init(args);
            if (!doRun) {
                System.exit(0);
            }
            result = appMaster.run();
        } catch (Throwable t) {
            LOG.log(Level.SEVERE, "Error running JBossApplicationMaster", t);
            System.exit(1);
        }
        if (result) {
            LOG.info("Application Master completed successfully. exiting");
            System.exit(0);
        } else {
            LOG.info("Application Master failed. exiting");
            System.exit(2);
        }
    }

    /**
     * Dump out contents of $CWD and the environment to stdout for debugging
     */
    private void dumpOutDebugInfo() {

        LOG.info("Dump debug output");
        Map<String, String> envs = System.getenv();
        for (Map.Entry<String, String> env : envs.entrySet()) {
            LOG.info("System env: key=" + env.getKey() + ", val="
                    + env.getValue());
            System.out.println("System env: key=" + env.getKey() + ", val="
                    + env.getValue());
        }

        String cmd = "ls -al";
        Runtime run = Runtime.getRuntime();
        Process pr = null;
        try {
            pr = run.exec(cmd);
            pr.waitFor();

            BufferedReader buf = new BufferedReader(new InputStreamReader(
                    pr.getInputStream()));
            String line = "";
            while ((line = buf.readLine()) != null) {
                LOG.info("System CWD content: " + line);
                System.out.println("System CWD content: " + line);
            }
            buf.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /*定义了一个默认的构造函数，进行ApplicationMaster的一些准备工作*/
    public JBossApplicationMaster() throws Exception {
        conf = new YarnConfiguration();
    }

    /**
     * Parse command line options
     *
     * @param args
     *            Command line args
     * @return Whether init successful and run should be invoked
     * @throws ParseException
     * @throws IOException
     */
    public boolean init(String[] args) throws ParseException, IOException {
        // 命令行参数
        Options opts = new Options();
        opts.addOption("app_attempt_id", true,
                "App Attempt ID. Not to be used unless for testing purposes");
        opts.addOption("admin_user", true,
                "User id for initial administrator user");
        opts.addOption("admin_password", true,
                "Password for initial administrator user");
        opts.addOption("container_memory", true,
                "Amount of memory in MB to be requested to run the shell command");
        opts.addOption("num_containers", true,
                "No. of containers on which the shell command needs to be executed");
        opts.addOption("jar", true, "JAR file containing the application");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("debug", false, "Dump out debug information");

        opts.addOption("help", false, "Print usage");

        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (args.length == 0) {
            printUsage(opts);
            throw new IllegalArgumentException(
                    "No args specified for application master to initialize");
        }

        if (cliParser.hasOption("help")) {
            printUsage(opts);
            return false;
        }

        if (cliParser.hasOption("debug")) {
            dumpOutDebugInfo();
        }

        Map<String, String> envs = System.getenv();

        ContainerId containerId = ConverterUtils.toContainerId(envs
                .get(Environment.CONTAINER_ID.name()));

        if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
            if (cliParser.hasOption("app_attempt_id")) {
                String appIdStr = cliParser
                        .getOptionValue("app_attempt_id", "");
                appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
            } else {
                throw new IllegalArgumentException(
                        "Application Attempt Id not set in the environment");
            }
        } else {
            containerId = ConverterUtils.toContainerId(envs
                    .get(Environment.CONTAINER_ID.name()));
            appAttemptID = containerId.getApplicationAttemptId();
        }

        if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
            throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV
                    + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HOST.name())) {
            throw new RuntimeException(Environment.NM_HOST.name()
                    + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
            throw new RuntimeException(Environment.NM_HTTP_PORT
                    + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_PORT.name())) {
            throw new RuntimeException(Environment.NM_PORT.name()
                    + " not set in the environment");
        }

        LOG.info("Application master for app" + ", appId="
                + appAttemptID.getApplicationId().getId()
                + ", clustertimestamp="
                + appAttemptID.getApplicationId().getClusterTimestamp()
                + ", attemptId=" + appAttemptID.getAttemptId());

        containerMemory = Integer.parseInt(cliParser.getOptionValue(
                "container_memory", "1024"));
        numTotalContainers = Integer.parseInt(cliParser.getOptionValue(
                "num_containers", "1"));

        adminUser = cliParser.getOptionValue("admin_user", "yarn");
        adminPassword = cliParser.getOptionValue("admin_password", "yarn");

        appJar = cliParser.getOptionValue("jar");
        if (numTotalContainers == 0) {
            throw new IllegalArgumentException(
                    "Cannot run JBoss Application Master with no containers");
        }
        requestPriority = Integer.parseInt(cliParser.getOptionValue("priority",
                "0"));

        return true;
    }

    /**
     * Helper function to print usage
     *
     * @param opts
     *            Parsed command line options
     */
    private void printUsage(Options opts) {
        new HelpFormatter().printHelp("JBossApplicationMaster", opts);
    }

    /**
     * Main run function for the application master
     *
     * @throws YarnException
     * @throws IOException
     * 实现一个回调函数处理器监听来自ResourceManager的事件
     * 通过Yarn api 创建一个对象，封装ApplicationMaster跟ResourceManager通信的客户端
     * 实现一个回调函数处理器监听来自NodeManager的事件
     * 通过Yarn API创建一个对象，封装ApplicationMaster跟NodeManager通信的客户端。
     * 实现一个类用来启动Container
     *
     * 初始化的对象有:RMCallbackHandler
     *              NMCallbackHandler
     *              RegisterApplicationMasterResponse
     *
     */
    @SuppressWarnings({ "unchecked" })
    public boolean run() throws YarnException, IOException {
        LOG.info("Starting JBossApplicationMaster");

        /*初始化ResourceManager回调函数处理器，然后用这个处理器创建一个Client对象。
        这个类是Yarn所提供的用于ApplicationMaster与ResourceManager异步通信的库，
        启动Container的类会被ResourceManager回调函数处理器调用 */

        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();

        resourceManager = AMRMClientAsync.createAMRMClientAsync(1000,
                allocListener);

        resourceManager.init(conf); // 服务首先进行初始化，初始化完成之后进行启动
        resourceManager.start();

        /*yarn service生命周期，先以YarnConfiguration对象为参数调用init方法，然后调用start方法。
        * NoeManager回调函数处理器的实例化，这个对象用来创建一个Client对象，这个对象用于ApplicationMaster和NodeManager
        * 之间进行异步通信，对这个对象，再一次重复了Yarn服务的调用模式*/
        containerListener = new NMCallbackHandler();
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf); // 服务首先进行初始化，初始化完成之后进行启动
        nmClientAsync.start();

        /*建立好回调函数处理器和异步通信的客户端对象之后，就可以向ResourceManager注册ApplicationMaster了
        *
        * 向ResourceManager注册ApplicationMaster时，需要提供一些基本的信息，
        *   ApplicationMaster的hostname
        *   PRC端口
        *   tracking URL(可以有开发者自己定义)Yarn的WEB界面上会对每个应用程序显示这个URL的链接，
        * 注册成功以后，ApplicationMaster上跟ResourceManager通信的心跳线程就启动了
        * */
        RegisterApplicationMasterResponse response = resourceManager
                .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
                        appMasterTrackingUrl);

        int maxMem = response.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

        // A resource ask cannot exceed the max.
        if (containerMemory > maxMem) {
            LOG.info("Container memory specified above max threshold of cluster."
                    + " Using max value."
                    + ", specified="
                    + containerMemory
                    + ", max=" + maxMem);
            containerMemory = maxMem;
        }

        for (int i = 0; i < numTotalContainers; ++i) {
            ContainerRequest containerAsk = setupContainerAskForRM();
            resourceManager.addContainerRequest(containerAsk);
        }

        numRequestedContainers.set(numTotalContainers);

        while (!done) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {
            }
        }
        finish();

        return success;
    }

    private void finish() {
        for (Thread launchThread : launchThreads) {
            try {
                launchThread.join(10000);
            } catch (InterruptedException e) {
                LOG.info("Exception thrown in thread join: " + e.getMessage());
                e.printStackTrace();
            }
        }

        LOG.info("Application completed. Stopping running containers");
        nmClientAsync.stop();

        LOG.info("Application completed. Signalling finish to RM");

        FinalApplicationStatus appStatus;
        String appMessage = null;
        success = true;
        if (numFailedContainers.get() == 0
                && numCompletedContainers.get() == numTotalContainers) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = "Diagnostics." + ", total=" + numTotalContainers
                    + ", completed=" + numCompletedContainers.get()
                    + ", allocated=" + numAllocatedContainers.get()
                    + ", failed=" + numFailedContainers.get();
            success = false;
        }
        try {
            resourceManager.unregisterApplicationMaster(appStatus, appMessage,
                    null);
        } catch (YarnException ex) {
            LOG.log(Level.SEVERE, "Failed to unregister application", ex);
        } catch (IOException e) {
            LOG.log(Level.SEVERE, "Failed to unregister application", e);
        }

        done = true;
        resourceManager.stop();
    }

    // ResourceManager的回调函数 定义为外部类也可以。
    /*
    * 类中实现方法都是在CallBackHandler中定义的
    * */
    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
        @SuppressWarnings("unchecked")
        /**
         * 一旦ResourceManager给分配了一个Container，就通过一个LaunchContainerRunnable类来启动刚分配的Container。
         * LaunchContainerRunnable实现了java.lang.Runnable接口，通过LaunchContainerRunnable类创建线程。
         * */

        public void onContainersCompleted(
                List<ContainerStatus> completedContainers) {
            LOG.info("Got response from RM for container ask, completedCnt="
                    + completedContainers.size());

            for (ContainerStatus containerStatus : completedContainers) {
                LOG.info("Got container status for containerID="
                        + containerStatus.getContainerId() + ", state="
                        + containerStatus.getState() + ", exitStatus="
                        + containerStatus.getExitStatus() + ", diagnostics="
                        + containerStatus.getDiagnostics());

                assert (containerStatus.getState() == ContainerState.COMPLETE);

                int exitStatus = containerStatus.getExitStatus();

                if (0 != exitStatus) {
                    if (ContainerExitStatus.ABORTED != exitStatus) {
                        numCompletedContainers.incrementAndGet();
                        numFailedContainers.incrementAndGet();
                    } else {
                        numAllocatedContainers.decrementAndGet();
                        numRequestedContainers.decrementAndGet();
                    }
                } else {
                    numCompletedContainers.incrementAndGet();
                    LOG.info("Container completed successfully."
                            + ", containerId="
                            + containerStatus.getContainerId());
                }
            }

            int askCount = numTotalContainers - numRequestedContainers.get();
            numRequestedContainers.addAndGet(askCount);

            if (askCount > 0) {
                for (int i = 0; i < askCount; ++i) {
                    ContainerRequest containerAsk = setupContainerAskForRM();
                    resourceManager.addContainerRequest(containerAsk);
                }
            }

            if (numCompletedContainers.get() == numTotalContainers) {
                done = true;
            }
        }

        public void onContainersAllocated(List<Container> allocatedContainers) {
            LOG.info("Got response from RM for container ask, allocatedCnt="
                    + allocatedContainers.size());
            numAllocatedContainers.addAndGet(allocatedContainers.size());
            for (Container allocatedContainer : allocatedContainers) {
                LOG.info("Launching shell command on a new container."
                        + ", containerId=" + allocatedContainer.getId()
                        + ", containerNode="
                        + allocatedContainer.getNodeId().getHost() + ":"
                        + allocatedContainer.getNodeId().getPort()
                        + ", containerNodeURI="
                        + allocatedContainer.getNodeHttpAddress()
                        + ", containerResourceMemory"
                        + allocatedContainer.getResource().getMemory());

                LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(
                        allocatedContainer, containerListener);
                Thread launchThread = new Thread(runnableLaunchContainer);

                launchThreads.add(launchThread);
                launchThread.start();
            }
        }

        public void onShutdownRequest() {
            done = true;
        }

        public void onNodesUpdated(List<NodeReport> updatedNodes) {
        }

        public float getProgress() {
            float progress = (float) numCompletedContainers.get()
                    / numTotalContainers;
            return progress;
        }

        public void onError(Throwable e) {
            done = true;
            resourceManager.stop();
        }
    }

    // NodeManager的回调函数
    private class NMCallbackHandler implements NMClientAsync.CallbackHandler {

        private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();

        public void addContainer(ContainerId containerId, Container container) {
            containers.putIfAbsent(containerId, container);
            LOG.info("Callback container id : " + containerId.toString());

            if (containers.size() == 1) {
                domainController = container.getNodeId().getHost();
            }
        }

        public void onContainerStopped(ContainerId containerId) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("Succeeded to stop Container " + containerId);
            }
            containers.remove(containerId);
        }

        public void onContainerStatusReceived(ContainerId containerId,
                                              ContainerStatus containerStatus) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("Container Status: id=" + containerId + ", status="
                        + containerStatus);
            }
        }

        public void onContainerStarted(ContainerId containerId,
                                       Map<String, ByteBuffer> allServiceResponse) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("Succeeded to start Container " + containerId);
            }
            Container container = containers.get(containerId);
            if (container != null) {
                nmClientAsync.getContainerStatusAsync(containerId,
                        container.getNodeId());
            }
        }

        public void onStartContainerError(ContainerId containerId, Throwable t) {
            LOG.log(Level.SEVERE, "Failed to start Container " + containerId);
            containers.remove(containerId);
        }

        public void onGetContainerStatusError(ContainerId containerId,
                                              Throwable t) {
            LOG.log(Level.SEVERE, "Failed to query the status of Container "
                    + containerId);
        }

        public void onStopContainerError(ContainerId containerId, Throwable t) {
            LOG.log(Level.SEVERE, "Failed to stop Container " + containerId);
            containers.remove(containerId);
        }

        public int getContainerCount() {
            return containers.size();
        }
    }

    /**
     * Thread to connect to the {@link ContainerManagementProtocol} and launch
     * the container that will execute the shell command.
     *
     * ApplicationMaster把Container的环境，启动命令和本地资源的设置都加到ContainerLaunchContext对象中，
     * 然后把这个对象提交给该Container所在的NodeManager这些工作完成以后，NodeManager就启动这个Container
     *
     * 创建一个LocalResource的Map，这个步骤和开发客户端时做的一样。
     * 把两个资源加入到这个LocalResource Map中，JBoss AS的发布包和整个Yarn应用程序的JAR包，
     * 创建一系列命令，通过这些命令，设置解压后的JBossAS文件的权限，修改JBossAS的配置文件，运行JBoss AS实例
     * 启动Container
     *
     */
    private class LaunchContainerRunnable implements Runnable {

        Container container;

        NMCallbackHandler containerListener;

        /**
         * @param lcontainer
         *            Allocated container
         * @param containerListener
         *            Callback handler of the container
         */
        public LaunchContainerRunnable(Container lcontainer,
                                       NMCallbackHandler containerListener) {
            this.container = lcontainer;
            this.containerListener = containerListener;
        }

        /**
         * Connects to CM, sets up container launch context for shell command
         * and eventually dispatches the container start request to the CM.
         */
        public void run() {

            String containerId = container.getId().toString();

            LOG.info("Setting up container launch container for containerid="
                    + container.getId());

            ContainerLaunchContext ctx = Records
                    .newRecord(ContainerLaunchContext.class);

            ctx.setEnvironment(shellEnv);

            Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

            String applicationId = container.getId().getApplicationAttemptId()
                    .getApplicationId().toString();
            try {
                FileSystem fs = FileSystem.get(conf);

                LocalResource jbossDist = Records
                        .newRecord(LocalResource.class);
                jbossDist.setType(LocalResourceType.ARCHIVE);
                jbossDist.setVisibility(LocalResourceVisibility.APPLICATION);

                Path jbossDistPath = new Path(new URI(
                        JBossConstants.JBOSS_DIST_PATH));
                jbossDist.setResource(ConverterUtils
                        .getYarnUrlFromPath(jbossDistPath));

                jbossDist.setTimestamp(fs.getFileStatus(jbossDistPath)
                        .getModificationTime());
                jbossDist.setSize(fs.getFileStatus(jbossDistPath).getLen());
                localResources.put(JBossConstants.JBOSS_SYMLINK, jbossDist);

                LocalResource jbossConf = Records
                        .newRecord(LocalResource.class);
                jbossConf.setType(LocalResourceType.FILE);
                jbossConf.setVisibility(LocalResourceVisibility.APPLICATION);

                Path jbossConfPath = new Path(new URI(appJar));
                jbossConf.setResource(ConverterUtils
                        .getYarnUrlFromPath(jbossConfPath));

                jbossConf.setTimestamp(fs.getFileStatus(jbossConfPath)
                        .getModificationTime());
                jbossConf.setSize(fs.getFileStatus(jbossConfPath).getLen());
                localResources.put(JBossConstants.JBOSS_ON_YARN_APP, jbossConf);

            } catch (Exception e) {
                LOG.log(Level.SEVERE, "Problem setting local resources", e);
                numCompletedContainers.incrementAndGet();
                numFailedContainers.incrementAndGet();
                return;
            }

            ctx.setLocalResources(localResources);

            List<String> commands = new ArrayList<String>();

            String host = container.getNodeId().getHost();

            String containerHome = conf.get("yarn.nodemanager.local-dirs")
                    + File.separator + ContainerLocalizer.USERCACHE
                    + File.separator
                    + System.getenv().get(Environment.USER.toString())
                    + File.separator + ContainerLocalizer.APPCACHE
                    + File.separator + applicationId + File.separator
                    + containerId;
            jbossHome = containerHome + File.separator
                    + JBossConstants.JBOSS_SYMLINK + File.separator
                    + JBossConstants.JBOSS_VERSION;

            String jbossPermissionsCommand = String.format("chmod -R 777 %s",
                    jbossHome);

            int portOffset = 0;
            int containerCount = containerListener.getContainerCount();
            if (containerCount > 1) {
                portOffset = containerCount * 150;
            }

            String domainControllerValue;
            if (domainController == null) {
                domainControllerValue = host;
            } else {
                domainControllerValue = domainController;
            }

            String jbossConfigurationCommand = String
                    .format("%s/bin/java -cp %s %s --home %s --server_group %s --server %s --port_offset %s --admin_user %s --admin_password %s --domain_controller %s --host %s",
                            Environment.JAVA_HOME.$(),
                            "/opt/hadoop-2.1.0-beta/share/hadoop/common/lib/*"
                                    + File.pathSeparator + containerHome
                                    + File.separator
                                    + JBossConstants.JBOSS_ON_YARN_APP,
                            JBossConfiguration.class.getName(), jbossHome,
                            applicationId, containerId, portOffset, adminUser,
                            adminPassword, domainControllerValue, host);

            LOG.info("Configuring JBoss on " + host + " with: "
                    + jbossConfigurationCommand);

            String jbossCommand = String
                    .format("%s%sbin%sdomain.sh -Djboss.bind.address=%s -Djboss.bind.address.management=%s -Djboss.bind.address.unsecure=%s",
                            jbossHome, File.separator, File.separator, host,
                            host, host);

            LOG.info("Starting JBoss with: " + jbossCommand);

            commands.add(jbossPermissionsCommand);
            commands.add(JBossConstants.COMMAND_CHAIN);
            commands.add(jbossConfigurationCommand);
            commands.add(JBossConstants.COMMAND_CHAIN);
            commands.add(jbossCommand);

            ctx.setCommands(commands);

            containerListener.addContainer(container.getId(), container);
            nmClientAsync.startContainerAsync(container, ctx);
        }
    }

    /**
     * Setup the request that will be sent to the RM for the container ask.
     *
     * @param numContainers
     *            Containers to ask for from RM
     * @return the setup ResourceRequest to be sent to RM
     */
    private ContainerRequest setupContainerAskForRM() {
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(requestPriority);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(containerMemory);
        capability.setVirtualCores(2);

        ContainerRequest request = new ContainerRequest(capability, null, null,
                pri);
        LOG.info("Requested container ask: " + request.toString());
        return request;
    }

}
