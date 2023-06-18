[jobmanager-io-thread-1] INFO org.apache.flink.runtime.jobmaster.JobMaster - Initializing job 'Flink Java Job at Tue Jun 13 20:37:14 CST 2023' (3fcf3167568c156ffd156330f4af4e4a).

1.JobMaster负责一个JobGraph执行管理

```java
/**
 * JobMaster implementation. The job master is responsible for the execution of a single {@link
 * JobGraph}.
 *
 * <p>It offers the following methods as part of its rpc interface to interact with the JobMaster
 * remotely:
 *
 * <ul>
 *   <li>{@link #updateTaskExecutionState} updates the task execution state for given task
 * </ul>
 */
public class JobMaster extends FencedRpcEndpoint<JobMasterId>
        implements JobMasterGateway, JobMasterService {
}
```

2.先看构造函数

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

    super(rpcService, RpcServiceUtils.createRandomName(JOB_MANAGER_NAME), jobMasterId);
    //...
    this.executionDeploymentTracker = executionDeploymentTracker;
    this.executionDeploymentReconciler =
            executionDeploymentReconcilerFactory.create(executionStateReconciliationHandler);

  //配置
    this.jobMasterConfiguration = checkNotNull(jobMasterConfiguration);
    this.resourceId = checkNotNull(resourceId);
  //jobGraph
    this.jobGraph = checkNotNull(jobGraph);
    this.rpcTimeout = jobMasterConfiguration.getRpcTimeout();
    this.highAvailabilityServices = checkNotNull(highAvailabilityService);
    this.blobWriter = jobManagerSharedServices.getBlobWriter();
    this.futureExecutor = jobManagerSharedServices.getFutureExecutor();
  //io
    this.ioExecutor = jobManagerSharedServices.getIoExecutor();
    this.jobCompletionActions = checkNotNull(jobCompletionActions);
    this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
  //代码loader
    this.userCodeLoader = checkNotNull(userCodeLoader);
    this.initializationTimestamp = initializationTimestamp;
    this.retrieveTaskManagerHostName =
            jobMasterConfiguration
                    .getConfiguration()
                    .getBoolean(JobManagerOptions.RETRIEVE_TASK_MANAGER_HOSTNAME);

    final String jobName = jobGraph.getName();
    final JobID jid = jobGraph.getJobID();

    log.info("Initializing job '{}' ({}).", jobName, jid);

    resourceManagerLeaderRetriever =
            highAvailabilityServices.getResourceManagerLeaderRetriever();
//预留TaskManagers注册
    this.registeredTaskManagers = new HashMap<>();
    this.blocklistHandler =
            blocklistHandlerFactory.create(
                    new JobMasterBlocklistContext(),
                    this::getNodeIdOfTaskManager,
                    getMainThreadExecutor(),
                    log);
//管理可利用slot
    this.slotPoolService =
            checkNotNull(slotPoolServiceSchedulerFactory)
                    .createSlotPoolService(
                            jid,
                            createDeclarativeSlotPoolFactory(
                                    jobMasterConfiguration.getConfiguration()));

    this.partitionTracker =
            checkNotNull(partitionTrackerFactory)
                    .create(
                            resourceID -> {
                                return Optional.ofNullable(
                                                registeredTaskManagers.get(resourceID))
                                        .map(TaskManagerRegistration::getTaskExecutorGateway);
                            });

    this.shuffleMaster = checkNotNull(shuffleMaster);

    this.jobManagerJobMetricGroup = jobMetricGroupFactory.create(jobGraph);
    this.jobStatusListener = new JobManagerJobStatusListener();

    this.failureEnrichers = checkNotNull(failureEnrichers);

    this.schedulerNG =
            createScheduler(
                    slotPoolServiceSchedulerFactory,
                    executionDeploymentTracker,
                    jobManagerJobMetricGroup,
                    jobStatusListener);
//心跳检查
    this.heartbeatServices = checkNotNull(heartbeatServices);
    this.taskManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();
    this.resourceManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();

    this.resourceManagerConnection = null;
    this.establishedResourceManagerConnection = null;

    this.accumulators = new HashMap<>();
}
```

2.看下启动流程

```java
@Override
protected void onStart() throws JobMasterException {
    try {
        startJobExecution();
    } catch (Exception e) {
        ...
    }
}
private void startJobExecution() throws Exception {
        validateRunsInMainThread();//校验是不是主线程 why?
        JobShuffleContext context = new JobShuffleContextImpl(jobGraph.getJobID(), this);
        shuffleMaster.registerJob(context);
        //启动一些对外服务
        startJobMasterServices();
        //待细看 
        startScheduling();
    }

private void startJobMasterServices() throws Exception {
        try {//两个心跳管理 对应 taskManager resourceManager
            this.taskManagerHeartbeatManager = createTaskManagerHeartbeatManager(heartbeatServices);
            this.resourceManagerHeartbeatManager =
                    createResourceManagerHeartbeatManager(heartbeatServices);

            // start the slot pool make sure the slot pool now accepts messages for this leader
            slotPoolService.start(getFencingToken(), getAddress(), getMainThreadExecutor());

            // job is ready to go, try to establish connection with resource manager
            //   - activate leader retrieval for the resource manager
            //   - on notification of the leader, the connection will be established and
            //     the slot pool will start requesting slots
           //与 resource manager 建立连接, 向 slot pool申请 slots
            resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());
        } catch (Exception e) {
            handleStartJobMasterServicesError(e);
        }
    }
```

3.重点看下startScheduling 

```java
private void startScheduling() {
    schedulerNG.startScheduling();
}
startScheduling 有两个实现 SchedulerBase AdaptiveScheduler
```

4.先看SchedulerBase的实现逻辑

```java
@Override
public final void startScheduling() {
    mainThreadExecutor.assertRunningInMainThread();//确认主线程
    registerJobMetrics(
            jobManagerJobMetricGroup,
            executionGraph,
            this::getNumberOfRestarts,
            deploymentStateTimeMetrics,
            executionGraph::registerJobStatusListener,
            executionGraph.getStatusTimestamp(JobStatus.INITIALIZING),
            jobStatusMetricsSettings);//注册指标
    operatorCoordinatorHandler.startAllOperatorCoordinators();//启动所有Operator的Coordinator
    startSchedulingInternal();
}
startSchedulingInternal 又有三个实现 看DefaultScheduler先
@Override
    protected void startSchedulingInternal() {
        log.info(
                "Starting scheduling with scheduling strategy [{}]",
                schedulingStrategy.getClass().getName());
        transitionToRunning();
        schedulingStrategy.startScheduling();
    }

protected final void transitionToRunning() {
        executionGraph.transitionToRunning(); //DefaultExecutionGraph.transitionToRunning
    }

@Override
    public void transitionToRunning() {
        if (!transitionState(JobStatus.CREATED, JobStatus.RUNNING)) {
            throw new IllegalStateException(
                    "Job may only be scheduled from state " + JobStatus.CREATED);
        }
    }//transitionState 做了状态转变和通知 notifyJobStatusChange(newState);notifyJobStatusHooks(newState, error);
```

5. 回到 schedulingStrategy.startScheduling(); 又有两个实现 PipelinedRegionSchedulingStrategy  VertexwiseSchedulingStrategy.
