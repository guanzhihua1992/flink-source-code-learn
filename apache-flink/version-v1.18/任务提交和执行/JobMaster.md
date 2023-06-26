JobMaster分别继承和实现了JobMasterService、 JobMasterGateway及FencedRpcEndpoint接口或抽象类，其中 JobMasterService接口定义了JobMaster启动和停止等方法， JobMasterGateway接口定义了JobMaster的RPC接口方法，如 heartbeatFromTaskManager()、heartbeatFromResourceManager() 等。JobMaster通过继承FencedRpcEndpoint抽象实现类，使得 JobMaster成为RPC服务节点，这样其他组件就可以通过RPC的通信方式 与JobMaster进行交互了。

·TaskExecutor和JobMaster之间需要创建RPC连接，TaskExecutor主 要调用offerSlot()和failSlot()方法向JobMaster提供分配的Slot信息以及通知 JobManager异常的Slot资源。TaskExecutor也会调用heartbeatFromTaskManager()、registerTaskManager()、 disconnectTaskManager()等方法维持与JobManager之间的RPC连接。

初始化

```java
public JobMaster(
        RpcService rpcService,
        JobMasterId jobMasterId,
        JobMasterConfiguration jobMasterConfiguration,
        ResourceID resourceId,
        JobGraph jobGraph,
        HighAvailabilityServices highAvailabilityService,
        SlotPoolServiceSchedulerFactory slotPoolServiceSchedulerFactory,
        JobManagerSharedServices jobManagerSharedServices,
        HeartbeatServices heartbeatServices,
        JobManagerJobMetricGroupFactory jobMetricGroupFactory,
        OnCompletionActions jobCompletionActions,
        FatalErrorHandler fatalErrorHandler,
        ClassLoader userCodeLoader,
        ShuffleMaster<?> shuffleMaster,
        PartitionTrackerFactory partitionTrackerFactory,
        ExecutionDeploymentTracker executionDeploymentTracker,
        ExecutionDeploymentReconciler.Factory executionDeploymentReconcilerFactory,
        BlocklistHandler.Factory blocklistHandlerFactory,
        Collection<FailureEnricher> failureEnrichers,
        long initializationTimestamp)
        throws Exception {
//通信服务
    super(rpcService, RpcServiceUtils.createRandomName(JOB_MANAGER_NAME), jobMasterId);

    final ExecutionDeploymentReconciliationHandler executionStateReconciliationHandler =
            new ExecutionDeploymentReconciliationHandler() {

                @Override
                public void onMissingDeploymentsOf(
                        Collection<ExecutionAttemptID> executionAttemptIds, ResourceID host) {
                    log.debug(
                            "Failing deployments {} due to no longer being deployed.",
                            executionAttemptIds);
                    for (ExecutionAttemptID executionAttemptId : executionAttemptIds) {
                        schedulerNG.updateTaskExecutionState(
                                new TaskExecutionState(
                                        executionAttemptId,
                                        ExecutionState.FAILED,
                                        new FlinkException(
                                                String.format(
                                                        "Execution %s is unexpectedly no longer running on task executor %s.",
                                                        executionAttemptId, host))));
                    }
                }

                @Override
                public void onUnknownDeploymentsOf(
                        Collection<ExecutionAttemptID> executionAttemptIds, ResourceID host) {
                    log.debug(
                            "Canceling left-over deployments {} on task executor {}.",
                            executionAttemptIds,
                            host);
                    for (ExecutionAttemptID executionAttemptId : executionAttemptIds) {
                        TaskManagerRegistration taskManagerRegistration =
                                registeredTaskManagers.get(host);
                        if (taskManagerRegistration != null) {
                            taskManagerRegistration
                                    .getTaskExecutorGateway()
                                    .cancelTask(executionAttemptId, rpcTimeout);
                        }
                    }
                }
            };

    this.executionDeploymentTracker = executionDeploymentTracker;
    this.executionDeploymentReconciler =
            executionDeploymentReconcilerFactory.create(executionStateReconciliationHandler);
//主要定义了JobMaster服务中需要的参 数，如rpcTimeout、slotRequestTimeout等
    this.jobMasterConfiguration = checkNotNull(jobMasterConfiguration);
  //JobMaster的唯一资源ID，用于区分不同的JobMaster 服务。
    this.resourceId = checkNotNull(resourceId);
  //当前需要提交的Job对应的JobGraph，从Dispatcher中 获取。
    this.jobGraph = checkNotNull(jobGraph);
  //定义JobMaster中RPC服务的超时时间。
    this.rpcTimeout = jobMasterConfiguration.getRpcTimeout();
  //高可用服务接口，主要用于获取 resourceManagerLeaderRetriever，可以通过resourceManagerLeaderRetriever 获取ResourceManager的Leader节点。
    this.highAvailabilityServices = checkNotNull(highAvailabilityService);
  //用于将对象数据写入BlobStore，主要用于Task调度 和执行过程中对TaskInformation进行持久化
    this.blobWriter = jobManagerSharedServices.getBlobWriter();
    this.futureExecutor = jobManagerSharedServices.getFutureExecutor();
    this.ioExecutor = jobManagerSharedServices.getIoExecutor();
  //主要定义了Job达到终止状态时执行的操 作，例如jobReachedGloballyTerminalState()定义了Job完成的操作。
    this.jobCompletionActions = checkNotNull(jobCompletionActions);
  //定义系统异常处理的Handle实现类。
    this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
  //JobGraph中对应的UserClassLoader实现类，主要 用于加载和实例化用户编写的Flink应用代码。
    this.userCodeLoader = checkNotNull(userCodeLoader);
    this.initializationTimestamp = initializationTimestamp;
  //存放ResourceManager的地址信息等内 容。
    this.retrieveTaskManagerHostName =
            jobMasterConfiguration
                    .getConfiguration()
                    .getBoolean(JobManagerOptions.RETRIEVE_TASK_MANAGER_HOSTNAME);

    final String jobName = jobGraph.getName();
    final JobID jid = jobGraph.getJobID();

    log.info("Initializing job '{}' ({}).", jobName, jid);
//用于获取ResourceManager Leader 节点的组件，可以通过resourceManagerLeaderRetriever实时监控当前 ResourceManager Leader节点的状态并返回最新的Leader节点
    resourceManagerLeaderRetriever =
            highAvailabilityServices.getResourceManagerLeaderRetriever();
//注册在JobManager中的TaskExecutor信息， 当有新的TaskExecutor启动时，通知JobLeaderService中的监听器，将 TaskExecutor注册在registeredTaskManagers集合中。
    this.registeredTaskManagers = new HashMap<>();
    this.blocklistHandler =
            blocklistHandlerFactory.create(
                    new JobMasterBlocklistContext(),
                    this::getNodeIdOfTaskManager,
                    getMainThreadExecutor(),
                    log);
//用于管理JobManager中的Slot资源，包括JobManager中 的资源使用、分配、申请以及TaskManager的注册和释放、接收 ResourceManager提供的Slot资源等。
    this.slotPoolService =
            checkNotNull(slotPoolServiceSchedulerFactory)
                    .createSlotPoolService(
                            jid,
                            createDeclarativeSlotPoolFactory(
                                    jobMasterConfiguration.getConfiguration()));
//主要用于对Job中的Partition信息进行追踪，并 提供startTrackingPartition()、stopTrackingAndReleasePartitions()等方法， 启动和释放TaskExecutor及ShuffleMaster中的Partition信息。
    this.partitionTracker =
            checkNotNull(partitionTrackerFactory)
                    .create(
                            resourceID -> {
                                return Optional.ofNullable(
                                                registeredTaskManagers.get(resourceID))
                                        .map(TaskManagerRegistration::getTaskExecutorGateway);
                            });
//主要用于注册和管理任务中的Shuffle信息。
    this.shuffleMaster = checkNotNull(shuffleMaster);

    this.jobManagerJobMetricGroup = jobMetricGroupFactory.create(jobGraph);
  //Job状态的监听器，实现当Job的状态发生改变 后的异步操作，例如在Job全部执行完毕后从高可用存储中移除当前的 Job信息等。
    this.jobStatusListener = new JobManagerJobStatusListener();

    this.failureEnrichers = checkNotNull(failureEnrichers);

    this.schedulerNG =
            createScheduler(
                    slotPoolServiceSchedulerFactory,
                    executionDeploymentTracker,
                    jobManagerJobMetricGroup,
                    jobStatusListener);
//创建和管理与TaskManager、ResourceManager组 件之间的心跳服务。
    this.heartbeatServices = checkNotNull(heartbeatServices);
  //管理与TaskManager之间的心跳信 息。
    this.taskManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();
  //管理与resourceManager之间的心跳信 息。
    this.resourceManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();
//创建与ResourceManager之间的RPC 连接。JobManager通过调用ResourceManagerGateway.registerJobManager() 方法将自己注册到ResourceManager中。
    this.resourceManagerConnection = null;
  //涵盖了JobManager与 ResourceManager之间的连接信息，主要包括ResourceManagerGateway和 ResourceID信息。
    this.establishedResourceManagerConnection = null;
//专门用于存储在Job中创建和用到的累加器。
    this.accumulators = new HashMap<>();
}
```

重点看看 createSlotPoolService createScheduler

启动

```java
@Override
protected void onStart() throws JobMasterException {
    try {
        startJobExecution();
    } catch (Exception e) {
        final JobMasterException jobMasterException =
                new JobMasterException("Could not start the JobMaster.", e);
        handleJobMasterError(jobMasterException);
        throw jobMasterException;
    }
}
```

startJobExecution

```java
private void startJobExecution() throws Exception {
    validateRunsInMainThread();//确保在主线程执行

    JobShuffleContext context = new JobShuffleContextImpl(jobGraph.getJobID(), this);
    shuffleMaster.registerJob(context);
//启动服务
    startJobMasterServices();

    log.info(
            "Starting execution of job '{}' ({}) under job master id {}.",
            jobGraph.getName(),
            jobGraph.getJobID(),
            getFencingToken());
//启动调度
    startScheduling();
}
```

startJobMasterServices

```java
private void startJobMasterServices() throws Exception {
    try {//两个HeartbeatManager
        this.taskManagerHeartbeatManager = createTaskManagerHeartbeatManager(heartbeatServices);
        this.resourceManagerHeartbeatManager =
                createResourceManagerHeartbeatManager(heartbeatServices);

        // start the slot pool make sure the slot pool now accepts messages for this leader
        slotPoolService.start(getFencingToken(), getAddress(), getMainThreadExecutor());

        // job is ready to go, try to establish connection with resource manager
        //   - activate leader retrieval for the resource manager
        //   - on notification of the leader, the connection will be established and
        //     the slot pool will start requesting slots
        resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());
    } catch (Exception e) {
        handleStartJobMasterServicesError(e);
    }
}
```