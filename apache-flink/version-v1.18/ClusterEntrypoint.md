ClusterEntrypoint

Flink的集群模式主要分为Per-Job和Session两种，其中Per-Job 集群模式为每一个提交的Job单独创建一套完整的运行时集群环境，该 Job独享运行时集群使用的计算资源以及组件服务。与Per-Job集群相 比，Session集群能够运行多个Flink作业，且这些作业可以共享运行 时中的Dispatcher、ResourceManager等组件服务。两种集群运行模式 各有特点和使用范围，从资源利用的角度看，Session集群资源的使用 率相对高一些，Per-Job集群任务之间的资源隔离性会好一些。

不管是哪种类型的集群，集群运行时环境中涉及的核心组件都是 一样的，主要的区别集中在作业的提交和运行过程中。

当用户指定Session Cli命令启动集群时，首先会在Flink集群启 动脚本中调用ClusterEntrypoint抽象实现类中提供的main()方法，以 启动和运行相应类型的集群环境。ClusterEntrypoint是整个集群运行 时的启动入口类，且内部带有main()方法。运行时管理节点中，所有 服务都通过ClusterEntrypoint进行触发和启动，进而完成核心组件的 创建和初始化。

ClusterEntrypoint会根据集群运行模式，将 ClusterEntrypoint分为SessionClusterEntrypoint和 ApplicationClusterEntryPoint两种基本实现类。顾名思义， SessionClusterEntrypoint是Session类型集群的入口启动类， ApplicationClusterEntryPoint是Per-Job类型集群的入口启动类。在集群运行 模式基本类的基础上，衍生出了集群资源管理器对应的 ClusterEntrypoint实现类，例如YarnApplicationClusterEntryPoint、 StandaloneApplicationClusterEntryPoint等。

从图3-2中可以看出，SessionClusterEntrypoint的实现类有 YarnSessionClusterEntrypoint、 StandaloneSessionClusterEntrypoint以及 KubernetesSessionClusterEntrypoint等。ApplicationClusterEntryPoint的实现类主要有YarnApplicationClusterEntryPoint、 StandaloneApplicationClusterEntryPoint和KubernetesApplicationClusterEntrypoint。用 户创建和启动的集群类型不同，最终通过不同的ClusterEntrypoint实 现类启动对应类型的集群运行环境。

我们以StandaloneSessionClusterEntrypoint为例 说明StandaloneSession集群的启动过程，并介绍其主要核心组件的创 建和初始化方法。

·用户运行start-cluster.sh命令启动StandaloneSession集群，此时在启 动脚本中会启动StandaloneSessionClusterEntrypoint入口类。

·在StandaloneSessionClusterEntrypoint.main方法中创建 StandaloneSessionClusterEntrypoint实例，然后将该实例传递至抽象实现 类ClusterEntrypoint.runClusterEntrypoint(entrypoint)方法继续后续流程。

·在ClusterEntrypoint中调用clusterEntrypoint.startCluster()方法启动 指定的ClusterEntrypoint实现类。

·调用基本实现类ClusterEntrypoint.runCluster()私有方法启动集群 服务和组件。

·调用ClusterEntrypoint.initializeServices(configuration)内部方法，初 始化运行时集群需要创建的基础组件服务，如HAServices、 CommonRPCService等。

·调用 ClusterEntrypoint.createDispatcherResourceManagerComponentFactory()子类 实现方法，创建DispatcherResourceManagerComponentFactory对象，在本 实例中会调用StandaloneSessionClusterEntrypoint实现的方法，其他类型的集群环境会根据不同实现，创建不同类型的 DispatcherResourceManager工厂类。

·在 StandaloneSessionClusterEntrypoint.createDispatcherResourceManagerCompo nentFacto-ry()方法中最终调用 DefaultDispatcherResourceManagerComponentFactory.createSessionCompon entFactory()方法，创建基于Session模式的DefaultDispatcherResourceMana- gerComponentFactory。

·基于前面创建的基础服务，调用 DispatcherResourceManagerComponentFactory.create()方法创建集群核心组 件封装类DispatcherResourceManagerComponent，可以看出核心组件实际 上包括了Dispatcher和ResourceManager。

·在DispatcherResourceManagerComponentFactory.create()方法中，首 先创建和启动WebMonitorEndpoint对象，作为Dispatcher对应的Rest endpoint，通过Rest API将JobGraph提交到Dispatcher上，同时， WebMonitorEndpoint也会提供Web UI需要的Rest API接口实现。

·调用ResourceManagerFactory.createResourceManager()方法创建 ResourceManager组件并启动。

·调用DispatcherRunnerFactory.createDispatcherRunner()方法创建 DispatcherRunner组件后，启动DispatcherRunner服务。

·将创建好的WebMonitorEndpoint、ResourceManager和 DispatcherRunner封装到DispatcherResourceManagerComponent中，其中 还包括DispatcherRunner和ResourceManager对应的高可用管理服务 dispatcherLeaderRetrievalService和resourceManagerRetrievalService。

StandaloneSessionClusterEntrypoint

```java
/** Entry point for the standalone session cluster. */
public class StandaloneSessionClusterEntrypoint extends SessionClusterEntrypoint {

    public StandaloneSessionClusterEntrypoint(Configuration configuration) {
        super(configuration);
    }

    @Override
    protected DefaultDispatcherResourceManagerComponentFactory
            createDispatcherResourceManagerComponentFactory(Configuration configuration) {
        return DefaultDispatcherResourceManagerComponentFactory.createSessionComponentFactory(
                StandaloneResourceManagerFactory.getInstance());
    }

    public static void main(String[] args) {
        // startup checks and logging
        EnvironmentInformation.logEnvironmentInfo(
                LOG, StandaloneSessionClusterEntrypoint.class.getSimpleName(), args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);
//loadConfiguration 
        final EntrypointClusterConfiguration entrypointClusterConfiguration =
                ClusterEntrypointUtils.parseParametersOrExit(
                        args,
                        new EntrypointClusterConfigurationParserFactory(),
                        StandaloneSessionClusterEntrypoint.class);
        Configuration configuration = loadConfiguration(entrypointClusterConfiguration);
//new StandaloneSessionClusterEntrypoint
        StandaloneSessionClusterEntrypoint entrypoint =
                new StandaloneSessionClusterEntrypoint(configuration);
//runClusterEntrypoint
        ClusterEntrypoint.runClusterEntrypoint(entrypoint);
    }
}
```

ClusterEntrypoint.runClusterEntrypoint(entrypoint);

```java
public static void runClusterEntrypoint(ClusterEntrypoint clusterEntrypoint) {

    final String clusterEntrypointName = clusterEntrypoint.getClass().getSimpleName();
    try {
        clusterEntrypoint.startCluster();
    } catch (ClusterEntrypointException e) {
        LOG.error(
                String.format("Could not start cluster entrypoint %s.", clusterEntrypointName),
                e);
        System.exit(STARTUP_FAILURE_RETURN_CODE);
    }

    int returnCode;
    Throwable throwable = null;

    try {
        returnCode = clusterEntrypoint.getTerminationFuture().get().processExitCode();
    } catch (Throwable e) {
        throwable = ExceptionUtils.stripExecutionException(e);
        returnCode = RUNTIME_FAILURE_RETURN_CODE;
    }

    LOG.info(
            "Terminating cluster entrypoint process {} with exit code {}.",
            clusterEntrypointName,
            returnCode,
            throwable);
    System.exit(returnCode);
}
```

clusterEntrypoint.startCluster();

```java
public void startCluster() throws ClusterEntrypointException {
    LOG.info("Starting {}.", getClass().getSimpleName());

    try {
        FlinkSecurityManager.setFromConfiguration(configuration);
        PluginManager pluginManager =
                PluginUtils.createPluginManagerFromRootFolder(configuration);
        configureFileSystems(configuration, pluginManager);

        SecurityContext securityContext = installSecurityContext(configuration);

        ClusterEntrypointUtils.configureUncaughtExceptionHandler(configuration);
        securityContext.runSecured(
                (Callable<Void>)
                        () -> {
                            runCluster(configuration, pluginManager);

                            return null;
                        });
    } catch (Throwable t) {
        final Throwable strippedThrowable =
                ExceptionUtils.stripException(t, UndeclaredThrowableException.class);

        try {
            // clean up any partial state
            shutDownAsync(
                            ApplicationStatus.FAILED,
                            ShutdownBehaviour.GRACEFUL_SHUTDOWN,
                            ExceptionUtils.stringifyException(strippedThrowable),
                            false)
                    .get(
                            INITIALIZATION_SHUTDOWN_TIMEOUT.toMilliseconds(),
                            TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            strippedThrowable.addSuppressed(e);
        }

        throw new ClusterEntrypointException(
                String.format(
                        "Failed to initialize the cluster entrypoint %s.",
                        getClass().getSimpleName()),
                strippedThrowable);
    }
}
```

runCluster(configuration, pluginManager);

```java
private void runCluster(Configuration configuration, PluginManager pluginManager)
        throws Exception {
    synchronized (lock) {
      //完成集群需要的基 础服务初始化操作。
        initializeServices(configuration, pluginManager);

        // write host information into configuration
      //将创建和初始化RPC服务地址和端口配置写入configuration，以 便在接下来创建的组件中使用。
        configuration.setString(JobManagerOptions.ADDRESS, commonRpcService.getAddress());
        configuration.setInteger(JobManagerOptions.PORT, commonRpcService.getPort());
//调用createDispatcherResourceManagerComponentFactory()抽象方 法，创建对应集群的DispatcherResourceManagerComponentFactory。
        final DispatcherResourceManagerComponentFactory
                dispatcherResourceManagerComponentFactory =
                        createDispatcherResourceManagerComponentFactory(configuration);
//通过dispatcherResourceManagerComponentFactory的实现类创建 clusterComponent，也就是运行时中使用的组件服务。
        clusterComponent =
                dispatcherResourceManagerComponentFactory.create(
                        configuration,
                        resourceId.unwrap(),
                        ioExecutor,
                        commonRpcService,
                        haServices,
                        blobServer,
                        heartbeatServices,
                        delegationTokenManager,
                        metricRegistry,
                        executionGraphInfoStore,
                        new RpcMetricQueryServiceRetriever(
                                metricRegistry.getMetricQueryServiceRpcService()),
                        failureEnrichers,
                        this);
//向clusterComponent的ShutDownFuture对象中添加需要在集群停 止后执行的异步操作。
        clusterComponent
                .getShutDownFuture()
                .whenComplete(
                        (ApplicationStatus applicationStatus, Throwable throwable) -> {
                            if (throwable != null) {
                                shutDownAsync(
                                        ApplicationStatus.UNKNOWN,
                                        ShutdownBehaviour.GRACEFUL_SHUTDOWN,
                                        ExceptionUtils.stringifyException(throwable),
                                        false);
                            } else {
                                // This is the general shutdown path. If a separate more
                                // specific shutdown was
                                // already triggered, this will do nothing
                                shutDownAsync(
                                        applicationStatus,
                                        ShutdownBehaviour.GRACEFUL_SHUTDOWN,
                                        null,
                                        true);
                            }
                        });
    }
}
```

initializeServices(configuration, pluginManager);

```java
protected void initializeServices(Configuration configuration, PluginManager pluginManager)
        throws Exception {

    LOG.info("Initializing cluster services.");

    synchronized (lock) {
        resourceId =
                configuration
                        .getOptional(JobManagerOptions.JOB_MANAGER_RESOURCE_ID)
                        .map(
                                value ->
                                        DeterminismEnvelope.deterministicValue(
                                                new ResourceID(value)))
                        .orElseGet(
                                () ->
                                        DeterminismEnvelope.nondeterministicValue(
                                                ResourceID.generate()));

        LOG.debug(
                "Initialize cluster entrypoint {} with resource id {}.",
                getClass().getSimpleName(),
                resourceId);

        workingDirectory =
                ClusterEntrypointUtils.createJobManagerWorkingDirectory(
                        configuration, resourceId);

        LOG.info("Using working directory: {}.", workingDirectory);

        rpcSystem = RpcSystem.load(configuration);
//从configuration中获取配置的RPC地址和portRange参数，根据配 置地址和端口信息创建集群所需的公用commonRpcService服务。更新 configuration中的address和port配置，用于支持集群组件高可用服务。
        commonRpcService =
                RpcUtils.createRemoteRpcService(
                        rpcSystem,
                        configuration,
                        configuration.getString(JobManagerOptions.ADDRESS),
                        getRPCPortRange(configuration),
                        configuration.getString(JobManagerOptions.BIND_HOST),
                        configuration.getOptional(JobManagerOptions.RPC_BIND_PORT));

        JMXService.startInstance(configuration.getString(JMXServerOptions.JMX_SERVER_PORT));

        // update the configuration used to create the high availability services
        configuration.setString(JobManagerOptions.ADDRESS, commonRpcService.getAddress());
        configuration.setInteger(JobManagerOptions.PORT, commonRpcService.getPort());
//创建ioExecutor线程池，用于集群组件的I/O操作，如本地文件 数据读取和输出等。
        ioExecutor =
                Executors.newFixedThreadPool(
                        ClusterEntrypointUtils.getPoolSize(configuration),
                        new ExecutorThreadFactory("cluster-io"));
        delegationTokenManager =
                DefaultDelegationTokenManagerFactory.create(
                        configuration,
                        pluginManager,
                        commonRpcService.getScheduledExecutor(),
                        ioExecutor);
        // Obtaining delegation tokens and propagating them to the local JVM receivers in a
        // one-time fashion is required because BlobServer may connect to external file systems
        delegationTokenManager.obtainDelegationTokens();
      //创建并启动haService，向集群组件提供高可用支持，集群中的组 件都会通过haService创建高可用服务。
        haServices = createHaServices(configuration, ioExecutor, rpcSystem);
      //创建并启动blobServer，存储集群需要的Blob对象数据， blobServer中存储的数据能够被JobManager以及TaskManager访问，例如 JobGraph中的JAR包等数据。
        blobServer =
                BlobUtils.createBlobServer(
                        configuration,
                        Reference.borrowed(workingDirectory.unwrap().getBlobStorageDirectory()),
                        haServices.createBlobStore());
        blobServer.start();
        configuration.setString(BlobServerOptions.PORT, String.valueOf(blobServer.getPort()));
      //创建heartbeatServices，主要用于创建集群组件之间的心跳检测， 例如ResourceManager与JobManager之间的心跳服务。
        heartbeatServices = createHeartbeatServices(configuration);
        failureEnrichers = FailureEnricherUtils.getFailureEnrichers(configuration);
      //创建metricRegistry服务，用于注册集群监控指标收集。
        metricRegistry = createMetricRegistry(configuration, pluginManager, rpcSystem);

        final RpcService metricQueryServiceRpcService =
                MetricUtils.startRemoteMetricsRpcService(
                        configuration,
                        commonRpcService.getAddress(),
                        configuration.getString(JobManagerOptions.BIND_HOST),
                        rpcSystem);
        metricRegistry.startQueryService(metricQueryServiceRpcService, null);

        final String hostname = RpcUtils.getHostname(commonRpcService);

        processMetricGroup =
                MetricUtils.instantiateProcessMetricGroup(
                        metricRegistry,
                        hostname,
                        ConfigurationUtils.getSystemResourceMetricsProbingInterval(
                                configuration));
//创建executionGraphInfoStore服务，用于压缩并存储集群中的 ExecutionGraph，主要有FileArchivedExecutionGraphStore和 MemoryArchivedExecutionGraphStore两种实现类型。
        executionGraphInfoStore =
                createSerializableExecutionGraphStore(
                        configuration, commonRpcService.getScheduledExecutor());
    }
}
```