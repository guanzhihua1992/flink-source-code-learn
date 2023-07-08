作为集群资源管理组件，不同的Cluster集群资 源管理涉及的初始化过程也会有所不同.

ResourceManager实现类分两 类，一类支持动态资源管理，例如KubernetesResourceManager、 YarnResourceManager及MesosResourceManager，另一类不支持动态资 源管理，例如StandaloneResourceManager。支持动态资源管理的集群 类型，可以按需启动TaskManager资源，根据Job所需的资源请求动态启动TaskManager节点，这种资源管理方式不用担心资源浪费和资源动 态伸缩的问题。实现动态资源管理的ResourceManager需要继承 ActiveResourceManager基本实现类

创建 以ResourceManagerFactory为例createResourceManager

```java
public ResourceManager<T> createResourceManager(
        ResourceManagerProcessContext context, UUID leaderSessionId) throws Exception {
//先创建 resourceManagerRuntimeServices
    final ResourceManagerRuntimeServices resourceManagerRuntimeServices =
            createResourceManagerRuntimeServices(
                    context.getRmRuntimeServicesConfig(),
                    context.getRpcService(),
                    context.getHighAvailabilityServices(),
                    SlotManagerMetricGroup.create(
                            context.getMetricRegistry(), context.getHostname()));
//创建ResourceManager
    return createResourceManager(
            context.getRmConfig(),
            context.getResourceId(),
            context.getRpcService(),
            leaderSessionId,
            context.getHeartbeatServices(),
            context.getDelegationTokenManager(),
            context.getFatalErrorHandler(),
            context.getClusterInformation(),
            context.getWebInterfaceUrl(),
            ResourceManagerMetricGroup.create(
                    context.getMetricRegistry(), context.getHostname()),
            resourceManagerRuntimeServices,
            context.getIoExecutor());
}
//createResourceManagerRuntimeServices
private ResourceManagerRuntimeServices createResourceManagerRuntimeServices(
            ResourceManagerRuntimeServicesConfiguration rmRuntimeServicesConfig,
            RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices,
            SlotManagerMetricGroup slotManagerMetricGroup) {

        return ResourceManagerRuntimeServices.fromConfiguration(
                rmRuntimeServicesConfig,
                highAvailabilityServices,
                rpcService.getScheduledExecutor(),
                slotManagerMetricGroup);
    }
// 创建了两个服务  slotManager jobLeaderIdService
public static ResourceManagerRuntimeServices fromConfiguration(
            ResourceManagerRuntimeServicesConfiguration configuration,
            HighAvailabilityServices highAvailabilityServices,
            ScheduledExecutor scheduledExecutor,
            SlotManagerMetricGroup slotManagerMetricGroup) {

        final SlotManager slotManager =
                createSlotManager(configuration, scheduledExecutor, slotManagerMetricGroup);

        final JobLeaderIdService jobLeaderIdService =
                new DefaultJobLeaderIdService(
                        highAvailabilityServices, scheduledExecutor, configuration.getJobTimeout());

        return new ResourceManagerRuntimeServices(slotManager, jobLeaderIdService);
    }
// protected abstract 方法 createResourceManager 两个实现类 StandaloneResourceManagerFactory ActiveResourceManagerFactory
protected abstract ResourceManager<T> createResourceManager(
            Configuration configuration,
            ResourceID resourceId,
            RpcService rpcService,
            UUID leaderSessionId,
            HeartbeatServices heartbeatServices,
            DelegationTokenManager delegationTokenManager,
            FatalErrorHandler fatalErrorHandler,
            ClusterInformation clusterInformation,
            @Nullable String webInterfaceUrl,
            ResourceManagerMetricGroup resourceManagerMetricGroup,
            ResourceManagerRuntimeServices resourceManagerRuntimeServices,
            Executor ioExecutor)
            throws Exception;
```

ActiveResourceManagerFactory.createResourceManager

```java
@Override
public ResourceManager<WorkerType> createResourceManager(
        Configuration configuration,
        ResourceID resourceId,
        RpcService rpcService,
        UUID leaderSessionId,
        HeartbeatServices heartbeatServices,
        DelegationTokenManager delegationTokenManager,
        FatalErrorHandler fatalErrorHandler,
        ClusterInformation clusterInformation,
        @Nullable String webInterfaceUrl,
        ResourceManagerMetricGroup resourceManagerMetricGroup,
        ResourceManagerRuntimeServices resourceManagerRuntimeServices,
        Executor ioExecutor)
        throws Exception {

    final ThresholdMeter failureRater = createStartWorkerFailureRater(configuration);
    final Duration retryInterval =
            configuration.get(ResourceManagerOptions.START_WORKER_RETRY_INTERVAL);
    final Duration workerRegistrationTimeout =
            configuration.get(ResourceManagerOptions.TASK_MANAGER_REGISTRATION_TIMEOUT);
    final Duration previousWorkerRecoverTimeout =
            configuration.get(
                    ResourceManagerOptions.RESOURCE_MANAGER_PREVIOUS_WORKER_RECOVERY_TIMEOUT);
//createResourceManagerDriver 按照资源类型不同有不同实现 KubernetesResourceManagerFactory YarnResourceManagerFactory
  //new ActiveResourceManager  调用super ResourceManager 构造方法
    return new ActiveResourceManager<>(
            createResourceManagerDriver(
                    configuration, webInterfaceUrl, rpcService.getAddress()),
            configuration,
            rpcService,
            leaderSessionId,
            resourceId,
            heartbeatServices,
            delegationTokenManager,
            resourceManagerRuntimeServices.getSlotManager(),
            ResourceManagerPartitionTrackerImpl::new,
            BlocklistUtils.loadBlocklistHandlerFactory(configuration),
            resourceManagerRuntimeServices.getJobLeaderIdService(),
            clusterInformation,
            fatalErrorHandler,
            resourceManagerMetricGroup,
            failureRater,
            retryInterval,
            workerRegistrationTimeout,
            previousWorkerRecoverTimeout,
            ioExecutor);
}
//super() 对应rpc服务启动注册
public ResourceManager(
            RpcService rpcService,
            UUID leaderSessionId,
            ResourceID resourceId,
            HeartbeatServices heartbeatServices,
            DelegationTokenManager delegationTokenManager,
            SlotManager slotManager,
            ResourceManagerPartitionTrackerFactory clusterPartitionTrackerFactory,
            BlocklistHandler.Factory blocklistHandlerFactory,
            JobLeaderIdService jobLeaderIdService,
            ClusterInformation clusterInformation,
            FatalErrorHandler fatalErrorHandler,
            ResourceManagerMetricGroup resourceManagerMetricGroup,
            Time rpcTimeout,
            Executor ioExecutor) {

        super(
                rpcService,
                RpcServiceUtils.createRandomName(RESOURCE_MANAGER_NAME),
                ResourceManagerId.fromUuid(leaderSessionId));

        this.resourceId = checkNotNull(resourceId);
        this.heartbeatServices = checkNotNull(heartbeatServices);
        this.slotManager = checkNotNull(slotManager);
        this.jobLeaderIdService = checkNotNull(jobLeaderIdService);
        this.clusterInformation = checkNotNull(clusterInformation);
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
        this.resourceManagerMetricGroup = checkNotNull(resourceManagerMetricGroup);

        this.jobManagerRegistrations = new HashMap<>(4);
        this.jmResourceIdRegistrations = new HashMap<>(4);
        this.taskExecutors = new HashMap<>(8);
        this.taskExecutorGatewayFutures = new HashMap<>(8);
        this.blocklistHandler =
                blocklistHandlerFactory.create(
                        new ResourceManagerBlocklistContext(),
                        this::getNodeIdOfTaskManager,
                        getMainThreadExecutor(),
                        log);

        this.jobManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();
        this.taskManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();

        this.clusterPartitionTracker =
                checkNotNull(clusterPartitionTrackerFactory)
                        .get(
                                (taskExecutorResourceId, dataSetIds) ->
                                        taskExecutors
                                                .get(taskExecutorResourceId)
                                                .getTaskExecutorGateway()
                                                .releaseClusterPartitions(dataSetIds, rpcTimeout)
                                                .exceptionally(
                                                        throwable -> {
                                                            log.debug(
                                                                    "Request for release of cluster partitions belonging to data sets {} was not successful.",
                                                                    dataSetIds,
                                                                    throwable);
                                                            throw new CompletionException(
                                                                    throwable);
                                                        }));
        this.ioExecutor = ioExecutor;

        this.startedFuture = new CompletableFuture<>();

        this.delegationTokenManager = delegationTokenManager;

        this.resourceAllocator = getResourceAllocator();
    }
```

启动

```java
@Override
public final void onStart() throws Exception {
    try {
        log.info("Starting the resource manager.");
        startResourceManagerServices();
        startedFuture.complete(null);
    } catch (Throwable t) {
        final ResourceManagerException exception =
                new ResourceManagerException(
                        String.format("Could not start the ResourceManager %s", getAddress()),
                        t);
        onFatalError(exception);
        throw exception;
    }
}
```

```java
private void startResourceManagerServices() throws Exception {
    try {
        jobLeaderIdService.start(new JobLeaderIdActionsImpl());
//指标注册
        registerMetrics();
//心跳服务 taskManagerHeartbeatManager jobManagerHeartbeatManager
        startHeartbeatServices();
//slotManager
        slotManager.start(
                getFencingToken(),
                getMainThreadExecutor(),
                resourceAllocator,
                new ResourceEventListenerImpl(),
                blocklistHandler::isBlockedTaskManager);
      
        delegationTokenManager.start(this);

        initialize();
    } catch (Exception e) {
        handleStartResourceManagerServicesException(e);
    }
}
```

```java
private void startHeartbeatServices() {
    taskManagerHeartbeatManager =
            heartbeatServices.createHeartbeatManagerSender(
                    resourceId,
                    new TaskManagerHeartbeatListener(),
                    getMainThreadExecutor(),
                    log);

    jobManagerHeartbeatManager =
            heartbeatServices.createHeartbeatManagerSender(
                    resourceId,
                    new JobManagerHeartbeatListener(),
                    getMainThreadExecutor(),
                    log);
}
```

initialize

```java
@Override
protected void initialize() throws ResourceManagerException {
    try {
        resourceManagerDriver.initialize(
                this,
                new GatewayMainThreadExecutor(),
                ioExecutor,
                blocklistHandler::getAllBlockedNodeIds);
    } catch (Exception e) {
        throw new ResourceManagerException("Cannot initialize resource provider.", e);
    }
}
```

resourceManagerDriver.initialize()

```java
@Override
public final void initialize(
        ResourceEventHandler<WorkerType> resourceEventHandler,
        ScheduledExecutor mainThreadExecutor,
        Executor ioExecutor,
        BlockedNodeRetriever blockedNodeRetriever)
        throws Exception {
    this.resourceEventHandler = Preconditions.checkNotNull(resourceEventHandler);
    this.mainThreadExecutor = Preconditions.checkNotNull(mainThreadExecutor);
    this.ioExecutor = Preconditions.checkNotNull(ioExecutor);
    this.blockedNodeRetriever = Preconditions.checkNotNull(blockedNodeRetriever);

    initializeInternal();
}

/** Initialize the deployment specific components. 还是两个实现类 KubernetesResourceManagerDriver YarnResourceManagerDriver*/
    protected abstract void initializeInternal() throws Exception;
```

```java
@Override
protected void initializeInternal() throws Exception {
    isRunning = true;
    final YarnContainerEventHandler yarnContainerEventHandler = new YarnContainerEventHandler();
    try {
        resourceManagerClient =
                yarnResourceManagerClientFactory.createResourceManagerClient(
                        yarnHeartbeatIntervalMillis, yarnContainerEventHandler);
        resourceManagerClient.init(yarnConfig);
        resourceManagerClient.start();

        final RegisterApplicationMasterResponse registerApplicationMasterResponse =
                registerApplicationMaster();
        getContainersFromPreviousAttempts(registerApplicationMasterResponse);
        taskExecutorProcessSpecContainerResourcePriorityAdapter =
                new TaskExecutorProcessSpecContainerResourcePriorityAdapter(
                        registerApplicationMasterResponse.getMaximumResourceCapability(),
                        ExternalResourceUtils.getExternalResourceConfigurationKeys(
                                flinkConfig,
                                YarnConfigOptions.EXTERNAL_RESOURCE_YARN_CONFIG_KEY_SUFFIX));
    } catch (Exception e) {
        throw new ResourceManagerException("Could not start resource manager client.", e);
    }

    nodeManagerClient =
            yarnNodeManagerClientFactory.createNodeManagerClient(yarnContainerEventHandler);
    nodeManagerClient.init(yarnConfig);
    nodeManagerClient.start();
}
```

具体功能 后续细看

 registerJobMaster JobMaster 注册

registerTaskExecutor TaskExecutor  注册

sendSlotReport

heartbeatFromTaskManager

heartbeatFromJobManager

disconnectTaskManager

disconnectJobManager

declareRequiredResources

notifySlotAvailable

deregisterApplication

