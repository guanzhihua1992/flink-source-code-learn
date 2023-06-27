当JobMaster以及相关服务组件都启动完毕且从ResourceManager 中申请到Slot计算资源后，接下来调度和执行Job中的SubTask任务， 这个过程涉及SchedulerNG任务调度器的创建和ExecutionGraph的构建与执行

整个任务调度和执行的步骤如下。依据历史版本相关类和方法可能有差别

1)调用JobMaster.resetAndStartScheduler()方法，创建和分配 Task执行所需的调度器SchedulerNG接口实现类。这里需要注意的是，

在SchedulerNG的创建过程中，会根据JobGraph、ShuffleMaster以及 JobMasterPartitionTracker等参数创建ExecutionGraph物理执行图。

2)调用SchedulerNG的基本实现类 SchedulerBase.startScheduling()方法启动任务调度，开始对整个 ExecutionGraph进行调度执行。

3)调用SchedulerBase.startSchedulingInternal()抽象方法 (主要有DefaultScheduler和LegacyScheduler两种调度器实现该抽象 方法，这里我们以DefaultScheduler为例进行说明)。

4)在DefaultScheduler中会使用不同的调度策略，分别为 EagerSchedulingStrategy和LazyFromSourcesSchedulingStrategy。 从字面上也可以看出，前者是即时调度策略，即Job中所有的Task会被 立即调度，主要用于Streaming类型的作业;后者是等待输入数据全部 准备好才开始后续的Task调度，主要用于Batch类型的作业。

5)在SchedulingStrategy中创建ExecutionVertexID和 ExecutionVertexDeploymentOption集合，然后将 ExecutionVertexDeploymentOption集合分配给SchedulerOperations 执行。

6)在DefaultScheduler中根据 ExecutionVertexDeploymentOption集合，将分配的Slot资源和 ExecutionVertex绑定，生成SlotExecutionVertexAssignment集合。 然后创建DeploymentHandle集合，同时调用deployIndividually()内 部方法执行DeploymentHandle集合中的Execution节点，其中 DeploymentHandle包含了部署ExecutionVertex需要的全部信息。

7)根据Job的调度策略是否为 LazyFromSourcesSchedulingStrategy，选择在DefaultScheduler中调 用deployIndividually()方法还是waitForAllSlotsAnddeploy()方 法，这里使用deployIndividually()方法独立部署DeploymentHandle 的Execution节点。

8)调用DefaultScheduler.deployOrHandleError()方法对 Execution中的ExecutionVertex节点进行调度，并进行异常处理。

9)如果ExecutionVertexVersion等信息都符合预期，则调用 deployTaskSafe()方法，部署ExecutionVertexID对应的Task作业，在 deployTaskSafe()方法中首先通过executionVertexId获取 ExecutionVertex，其中ExecutionVertex是ExecutionGraph中的节 点，代表execution的一个并行SubTask。

10)调用ExecutionVertexOperations.deploy()方法执行该 ExecutionVertex。在ExecutionVertexOperations中默认实现 DefaultExecutionVertexOperations的deploy()方法，实际上是调用 ExecutionVertex.deploy()方法提交当前ExecutionVertex对应的Task 作业到TaskManager中执行。

11)在ExecutionVertex.deploy()方法中运行的是 currentExecution.deploy()方法，而在Execution.deploy()方法中， 首先会从Slot信息中抽取TaskManagerGateway对象，然后调用 TaskManagerGateway.submitTask()方法将创建的 TaskDeploymentDescriptor对象提交到TaskExecutor中运行，至此 Task就正式被提交到TaskExecutor上运行了。

12)异步将Task运行的状态汇报给SchedulerNG，这里主要采用监 听器实现，JobMaster从SchedulerNG中再次获取整个Job的执行状态。

可以看出，ExecutionGraph在JobMaster中调度和执行的过程是比 较复杂的，涉及非常多的组件和服务。我们做一个简单的总结，首先 在JobMaster中创建DefaultScheduler，在创建的同时将JobGraph结构 转换为ExecutionGraph，用于Task实例的调度。然后通过 DefaultScheduler对ExecutionGraph中的ExecutionVertex节点进行调 度和执行。最后将ExecutionVertex以Task的形式在TaskExecutor上运 行。

ExecutionGraph生成 默认实现类DefaultExecutionGraph 先看构造方法

```java
public DefaultExecutionGraph(
        JobInformation jobInformation,
        ScheduledExecutorService futureExecutor,
        Executor ioExecutor,
        Time rpcTimeout,
        int executionHistorySizeLimit,
        ClassLoader userClassLoader,
        BlobWriter blobWriter,
        PartitionGroupReleaseStrategy.Factory partitionGroupReleaseStrategyFactory,
        ShuffleMaster<?> shuffleMaster,
        JobMasterPartitionTracker partitionTracker,
        TaskDeploymentDescriptorFactory.PartitionLocationConstraint partitionLocationConstraint,
        ExecutionDeploymentListener executionDeploymentListener,
        ExecutionStateUpdateListener executionStateUpdateListener,
        long initializationTimestamp,
        VertexAttemptNumberStore initialAttemptCounts,
        VertexParallelismStore vertexParallelismStore,
        boolean isDynamic,
        ExecutionJobVertex.Factory executionJobVertexFactory,
        List<JobStatusHook> jobStatusHooks,
        MarkPartitionFinishedStrategy markPartitionFinishedStrategy,
        boolean nonFinishedHybridPartitionShouldBeUnknown)
        throws IOException {

    this.executionGraphId = new ExecutionGraphID();

    this.jobInformation = checkNotNull(jobInformation);

    this.blobWriter = checkNotNull(blobWriter);

    this.partitionLocationConstraint = checkNotNull(partitionLocationConstraint);

    this.jobInformationOrBlobKey =
            BlobWriter.serializeAndTryOffload(
                    jobInformation, jobInformation.getJobId(), blobWriter);

    this.futureExecutor = checkNotNull(futureExecutor);
    this.ioExecutor = checkNotNull(ioExecutor);

    this.userClassLoader = checkNotNull(userClassLoader, "userClassLoader");

    this.tasks = new HashMap<>(16);
    this.intermediateResults = new HashMap<>(16);
    this.verticesInCreationOrder = new ArrayList<>(16);
    this.currentExecutions = new HashMap<>(16);

    this.jobStatusListeners = new ArrayList<>();

    this.stateTimestamps = new long[JobStatus.values().length];
    this.stateTimestamps[JobStatus.INITIALIZING.ordinal()] = initializationTimestamp;
    this.stateTimestamps[JobStatus.CREATED.ordinal()] = System.currentTimeMillis();

    this.rpcTimeout = checkNotNull(rpcTimeout);

    this.partitionGroupReleaseStrategyFactory =
            checkNotNull(partitionGroupReleaseStrategyFactory);

    this.kvStateLocationRegistry =
            new KvStateLocationRegistry(jobInformation.getJobId(), getAllVertices());

    this.executionHistorySizeLimit = executionHistorySizeLimit;

    this.schedulingFuture = null;
    this.jobMasterMainThreadExecutor =
            new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor(
                    "ExecutionGraph is not initialized with proper main thread executor. "
                            + "Call to ExecutionGraph.start(...) required.");

    this.shuffleMaster = checkNotNull(shuffleMaster);

    this.partitionTracker = checkNotNull(partitionTracker);

    this.resultPartitionAvailabilityChecker =
            new ExecutionGraphResultPartitionAvailabilityChecker(
                    this::createResultPartitionId, partitionTracker);

    this.executionDeploymentListener = executionDeploymentListener;
    this.executionStateUpdateListener = executionStateUpdateListener;

    this.initialAttemptCounts = initialAttemptCounts;

    this.parallelismStore = vertexParallelismStore;

    this.edgeManager = new EdgeManager();
    this.executionVerticesById = new HashMap<>();
    this.resultPartitionsById = new HashMap<>();
    this.vertexInputInfoStore = new VertexInputInfoStore();

    this.isDynamic = isDynamic;

    this.executionJobVertexFactory = checkNotNull(executionJobVertexFactory);

    this.jobStatusHooks = checkNotNull(jobStatusHooks);

    this.markPartitionFinishedStrategy = markPartitionFinishedStrategy;

    this.nonFinishedHybridPartitionShouldBeUnknown = nonFinishedHybridPartitionShouldBeUnknown;

    LOG.info(
            "Created execution graph {} for job {}.",
            executionGraphId,
            jobInformation.getJobId());
    // Trigger hook onCreated
    notifyJobStatusHooks(state, null);
}
```

起点 JobMaster.createScheduler()

```java
private SchedulerNG createScheduler(
        SlotPoolServiceSchedulerFactory slotPoolServiceSchedulerFactory,
        ExecutionDeploymentTracker executionDeploymentTracker,
        JobManagerJobMetricGroup jobManagerJobMetricGroup,
        JobStatusListener jobStatusListener)
        throws Exception {
    final SchedulerNG scheduler =
            slotPoolServiceSchedulerFactory.createScheduler(
                    log,
                    jobGraph,
                    ioExecutor,
                    jobMasterConfiguration.getConfiguration(),
                    slotPoolService,
                    futureExecutor,
                    userCodeLoader,
                    highAvailabilityServices.getCheckpointRecoveryFactory(),
                    rpcTimeout,
                    blobWriter,
                    jobManagerJobMetricGroup,
                    jobMasterConfiguration.getSlotRequestTimeout(),
                    shuffleMaster,
                    partitionTracker,
                    executionDeploymentTracker,
                    initializationTimestamp,
                    getMainThreadExecutor(),
                    fatalErrorHandler,
                    jobStatusListener,
                    failureEnrichers,
                    blocklistHandler::addNewBlockedNodes);

    return scheduler;
}
```

DefaultSlotPoolServiceSchedulerFactory.createScheduler

```java
@Override
public SchedulerNG createScheduler(
        Logger log,
        JobGraph jobGraph,
        Executor ioExecutor,
        Configuration configuration,
        SlotPoolService slotPoolService,
        ScheduledExecutorService futureExecutor,
        ClassLoader userCodeLoader,
        CheckpointRecoveryFactory checkpointRecoveryFactory,
        Time rpcTimeout,
        BlobWriter blobWriter,
        JobManagerJobMetricGroup jobManagerJobMetricGroup,
        Time slotRequestTimeout,
        ShuffleMaster<?> shuffleMaster,
        JobMasterPartitionTracker partitionTracker,
        ExecutionDeploymentTracker executionDeploymentTracker,
        long initializationTimestamp,
        ComponentMainThreadExecutor mainThreadExecutor,
        FatalErrorHandler fatalErrorHandler,
        JobStatusListener jobStatusListener,
        Collection<FailureEnricher> failureEnrichers,
        BlocklistOperations blocklistOperations)
        throws Exception {
    return schedulerNGFactory.createInstance(
            log,
            jobGraph,
            ioExecutor,
            configuration,
            slotPoolService,
            futureExecutor,
            userCodeLoader,
            checkpointRecoveryFactory,
            rpcTimeout,
            blobWriter,
            jobManagerJobMetricGroup,
            slotRequestTimeout,
            shuffleMaster,
            partitionTracker,
            executionDeploymentTracker,
            initializationTimestamp,
            mainThreadExecutor,
            fatalErrorHandler,
            jobStatusListener,
            failureEnrichers,
            blocklistOperations);
}
```

DefaultSchedulerFactory.createInstance

```java
@Override
public SchedulerNG createInstance(
        final Logger log,
        final JobGraph jobGraph,
        final Executor ioExecutor,
        final Configuration jobMasterConfiguration,
        final SlotPoolService slotPoolService,
        final ScheduledExecutorService futureExecutor,
        final ClassLoader userCodeLoader,
        final CheckpointRecoveryFactory checkpointRecoveryFactory,
        final Time rpcTimeout,
        final BlobWriter blobWriter,
        final JobManagerJobMetricGroup jobManagerJobMetricGroup,
        final Time slotRequestTimeout,
        final ShuffleMaster<?> shuffleMaster,
        final JobMasterPartitionTracker partitionTracker,
        final ExecutionDeploymentTracker executionDeploymentTracker,
        long initializationTimestamp,
        final ComponentMainThreadExecutor mainThreadExecutor,
        final FatalErrorHandler fatalErrorHandler,
        final JobStatusListener jobStatusListener,
        final Collection<FailureEnricher> failureEnrichers,
        final BlocklistOperations blocklistOperations)
        throws Exception {

    final SlotPool slotPool =
            slotPoolService
                    .castInto(SlotPool.class)
                    .orElseThrow(
                            () ->
                                    new IllegalStateException(
                                            "The DefaultScheduler requires a SlotPool."));

    final DefaultSchedulerComponents schedulerComponents =
            createSchedulerComponents(
                    jobGraph.getJobType(),
                    jobGraph.isApproximateLocalRecoveryEnabled(),
                    jobMasterConfiguration,
                    slotPool,
                    slotRequestTimeout);
    final RestartBackoffTimeStrategy restartBackoffTimeStrategy =
            RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                            jobGraph.getSerializedExecutionConfig()
                                    .deserializeValue(userCodeLoader)
                                    .getRestartStrategy(),
                            jobMasterConfiguration,
                            jobGraph.isCheckpointingEnabled())
                    .create();
    log.info(
            "Using restart back off time strategy {} for {} ({}).",
            restartBackoffTimeStrategy,
            jobGraph.getName(),
            jobGraph.getJobID());

    final ExecutionGraphFactory executionGraphFactory =
            new DefaultExecutionGraphFactory(
                    jobMasterConfiguration,
                    userCodeLoader,
                    executionDeploymentTracker,
                    futureExecutor,
                    ioExecutor,
                    rpcTimeout,
                    jobManagerJobMetricGroup,
                    blobWriter,
                    shuffleMaster,
                    partitionTracker);

    return new DefaultScheduler(
            log,
            jobGraph,
            ioExecutor,
            jobMasterConfiguration,
            schedulerComponents.getStartUpAction(),
            new ScheduledExecutorServiceAdapter(futureExecutor),
            userCodeLoader,
            new CheckpointsCleaner(),
            checkpointRecoveryFactory,
            jobManagerJobMetricGroup,
            schedulerComponents.getSchedulingStrategyFactory(),
            FailoverStrategyFactoryLoader.loadFailoverStrategyFactory(jobMasterConfiguration),
            restartBackoffTimeStrategy,
            new DefaultExecutionOperations(),
            new ExecutionVertexVersioner(),
            schedulerComponents.getAllocatorFactory(),
            initializationTimestamp,
            mainThreadExecutor,
            (jobId, jobStatus, timestamp) -> {
                if (jobStatus == JobStatus.RESTARTING) {
                    slotPool.setIsJobRestarting(true);
                } else {
                    slotPool.setIsJobRestarting(false);
                }
                jobStatusListener.jobStatusChanges(jobId, jobStatus, timestamp);
            },
            failureEnrichers,
            executionGraphFactory,
            shuffleMaster,
            rpcTimeout,
            computeVertexParallelismStore(jobGraph),
            new DefaultExecutionDeployer.Factory());
}
```

new DefaultScheduler

```java
protected DefaultScheduler(
        final Logger log,
        final JobGraph jobGraph,
        final Executor ioExecutor,
        final Configuration jobMasterConfiguration,
        final Consumer<ComponentMainThreadExecutor> startUpAction,
        final ScheduledExecutor delayExecutor,
        final ClassLoader userCodeLoader,
        final CheckpointsCleaner checkpointsCleaner,
        final CheckpointRecoveryFactory checkpointRecoveryFactory,
        final JobManagerJobMetricGroup jobManagerJobMetricGroup,
        final SchedulingStrategyFactory schedulingStrategyFactory,
        final FailoverStrategy.Factory failoverStrategyFactory,
        final RestartBackoffTimeStrategy restartBackoffTimeStrategy,
        final ExecutionOperations executionOperations,
        final ExecutionVertexVersioner executionVertexVersioner,
        final ExecutionSlotAllocatorFactory executionSlotAllocatorFactory,
        long initializationTimestamp,
        final ComponentMainThreadExecutor mainThreadExecutor,
        final JobStatusListener jobStatusListener,
        final Collection<FailureEnricher> failureEnrichers,
        final ExecutionGraphFactory executionGraphFactory,
        final ShuffleMaster<?> shuffleMaster,
        final Time rpcTimeout,
        final VertexParallelismStore vertexParallelismStore,
        final ExecutionDeployer.Factory executionDeployerFactory)
        throws Exception {

    super(
            log,
            jobGraph,
            ioExecutor,
            jobMasterConfiguration,
            checkpointsCleaner,
            checkpointRecoveryFactory,
            jobManagerJobMetricGroup,
            executionVertexVersioner,
            initializationTimestamp,
            mainThreadExecutor,
            jobStatusListener,
            executionGraphFactory,
            vertexParallelismStore);

    this.log = log;

    this.delayExecutor = checkNotNull(delayExecutor);
    this.userCodeLoader = checkNotNull(userCodeLoader);
    this.executionOperations = checkNotNull(executionOperations);
    this.shuffleMaster = checkNotNull(shuffleMaster);

    this.reservedAllocationRefCounters = new HashMap<>();
    this.reservedAllocationByExecutionVertex = new HashMap<>();

    final FailoverStrategy failoverStrategy =
            failoverStrategyFactory.create(
                    getSchedulingTopology(), getResultPartitionAvailabilityChecker());
    log.info(
            "Using failover strategy {} for {} ({}).",
            failoverStrategy,
            jobGraph.getName(),
            jobGraph.getJobID());

    final Context taskFailureCtx =
            DefaultFailureEnricherContext.forTaskFailure(
                    jobGraph.getJobID(),
                    jobGraph.getName(),
                    jobManagerJobMetricGroup,
                    ioExecutor,
                    userCodeLoader);

    final Context globalFailureCtx =
            DefaultFailureEnricherContext.forGlobalFailure(
                    jobGraph.getJobID(),
                    jobGraph.getName(),
                    jobManagerJobMetricGroup,
                    ioExecutor,
                    userCodeLoader);

    this.executionFailureHandler =
            new ExecutionFailureHandler(
                    getSchedulingTopology(),
                    failoverStrategy,
                    restartBackoffTimeStrategy,
                    mainThreadExecutor,
                    failureEnrichers,
                    taskFailureCtx,
                    globalFailureCtx);

    this.schedulingStrategy =
            schedulingStrategyFactory.createInstance(this, getSchedulingTopology());

    this.executionSlotAllocator =
            checkNotNull(executionSlotAllocatorFactory)
                    .createInstance(new DefaultExecutionSlotAllocationContext());

    this.verticesWaitingForRestart = new HashSet<>();
    startUpAction.accept(mainThreadExecutor);

    this.executionDeployer =
            executionDeployerFactory.createInstance(
                    log,
                    executionSlotAllocator,
                    executionOperations,
                    executionVertexVersioner,
                    rpcTimeout,
                    this::startReserveAllocation,
                    mainThreadExecutor);
}
```

super

```java
public SchedulerBase(
        final Logger log,
        final JobGraph jobGraph,
        final Executor ioExecutor,
        final Configuration jobMasterConfiguration,
        final CheckpointsCleaner checkpointsCleaner,
        final CheckpointRecoveryFactory checkpointRecoveryFactory,
        final JobManagerJobMetricGroup jobManagerJobMetricGroup,
        final ExecutionVertexVersioner executionVertexVersioner,
        long initializationTimestamp,
        final ComponentMainThreadExecutor mainThreadExecutor,
        final JobStatusListener jobStatusListener,
        final ExecutionGraphFactory executionGraphFactory,
        final VertexParallelismStore vertexParallelismStore)
        throws Exception {

    this.log = checkNotNull(log);
    this.jobGraph = checkNotNull(jobGraph);
    this.executionGraphFactory = executionGraphFactory;

    this.jobManagerJobMetricGroup = checkNotNull(jobManagerJobMetricGroup);
    this.executionVertexVersioner = checkNotNull(executionVertexVersioner);
    this.mainThreadExecutor = mainThreadExecutor;

    this.checkpointsCleaner = checkpointsCleaner;
    this.completedCheckpointStore =
            SchedulerUtils.createCompletedCheckpointStoreIfCheckpointingIsEnabled(
                    jobGraph,
                    jobMasterConfiguration,
                    checkNotNull(checkpointRecoveryFactory),
                    ioExecutor,
                    log);
    this.checkpointIdCounter =
            SchedulerUtils.createCheckpointIDCounterIfCheckpointingIsEnabled(
                    jobGraph, checkNotNull(checkpointRecoveryFactory));

    this.jobStatusMetricsSettings =
            MetricOptions.JobStatusMetricsSettings.fromConfiguration(jobMasterConfiguration);
    this.deploymentStateTimeMetrics =
            new DeploymentStateTimeMetrics(jobGraph.getJobType(), jobStatusMetricsSettings);

    this.executionGraph =
            createAndRestoreExecutionGraph(
                    completedCheckpointStore,
                    checkpointsCleaner,
                    checkpointIdCounter,
                    initializationTimestamp,
                    mainThreadExecutor,
                    jobStatusListener,
                    vertexParallelismStore);

    this.schedulingTopology = executionGraph.getSchedulingTopology();

    stateLocationRetriever =
            executionVertexId ->
                    getExecutionVertex(executionVertexId).getPreferredLocationBasedOnState();
    inputsLocationsRetriever =
            new ExecutionGraphToInputsLocationsRetrieverAdapter(executionGraph);

    this.kvStateHandler = new KvStateHandler(executionGraph);
    this.executionGraphHandler =
            new ExecutionGraphHandler(executionGraph, log, ioExecutor, this.mainThreadExecutor);

    this.operatorCoordinatorHandler =
            new DefaultOperatorCoordinatorHandler(executionGraph, this::handleGlobalFailure);
    operatorCoordinatorHandler.initializeOperatorCoordinators(
            this.mainThreadExecutor, jobManagerJobMetricGroup);

    this.exceptionHistory =
            new BoundedFIFOQueue<>(
                    jobMasterConfiguration.getInteger(WebOptions.MAX_EXCEPTION_HISTORY_SIZE));
}
```

createAndRestoreExecutionGraph

```java
private ExecutionGraph createAndRestoreExecutionGraph(
        CompletedCheckpointStore completedCheckpointStore,
        CheckpointsCleaner checkpointsCleaner,
        CheckpointIDCounter checkpointIdCounter,
        long initializationTimestamp,
        ComponentMainThreadExecutor mainThreadExecutor,
        JobStatusListener jobStatusListener,
        VertexParallelismStore vertexParallelismStore)
        throws Exception {

    final ExecutionGraph newExecutionGraph =
            executionGraphFactory.createAndRestoreExecutionGraph(
                    jobGraph,
                    completedCheckpointStore,
                    checkpointsCleaner,
                    checkpointIdCounter,
                    TaskDeploymentDescriptorFactory.PartitionLocationConstraint.fromJobType(
                            jobGraph.getJobType()),
                    initializationTimestamp,
                    new DefaultVertexAttemptNumberStore(),
                    vertexParallelismStore,
                    deploymentStateTimeMetrics,
                    getMarkPartitionFinishedStrategy(),
                    log);

    newExecutionGraph.setInternalTaskFailuresListener(
            new UpdateSchedulerNgOnInternalFailuresListener(this));
    newExecutionGraph.registerJobStatusListener(jobStatusListener);
    newExecutionGraph.start(mainThreadExecutor);

    return newExecutionGraph;
}
```

ExecutionGraphFactory.createAndRestoreExecutionGraph

```java
@Override
public ExecutionGraph createAndRestoreExecutionGraph(
        JobGraph jobGraph,
        CompletedCheckpointStore completedCheckpointStore,
        CheckpointsCleaner checkpointsCleaner,
        CheckpointIDCounter checkpointIdCounter,
        TaskDeploymentDescriptorFactory.PartitionLocationConstraint partitionLocationConstraint,
        long initializationTimestamp,
        VertexAttemptNumberStore vertexAttemptNumberStore,
        VertexParallelismStore vertexParallelismStore,
        ExecutionStateUpdateListener executionStateUpdateListener,
        MarkPartitionFinishedStrategy markPartitionFinishedStrategy,
        Logger log)
        throws Exception {
    ExecutionDeploymentListener executionDeploymentListener =
            new ExecutionDeploymentTrackerDeploymentListenerAdapter(executionDeploymentTracker);
    ExecutionStateUpdateListener combinedExecutionStateUpdateListener =
            (execution, previousState, newState) -> {
                executionStateUpdateListener.onStateUpdate(execution, previousState, newState);
                if (newState.isTerminal()) {
                    executionDeploymentTracker.stopTrackingDeploymentOf(execution);
                }
            };

    final ExecutionGraph newExecutionGraph =
            DefaultExecutionGraphBuilder.buildGraph(
                    jobGraph,
                    configuration,
                    futureExecutor,
                    ioExecutor,
                    userCodeClassLoader,
                    completedCheckpointStore,
                    checkpointsCleaner,
                    checkpointIdCounter,
                    rpcTimeout,
                    blobWriter,
                    log,
                    shuffleMaster,
                    jobMasterPartitionTracker,
                    partitionLocationConstraint,
                    executionDeploymentListener,
                    combinedExecutionStateUpdateListener,
                    initializationTimestamp,
                    vertexAttemptNumberStore,
                    vertexParallelismStore,
                    checkpointStatsTrackerFactory,
                    isDynamicGraph,
                    executionJobVertexFactory,
                    markPartitionFinishedStrategy,
                    nonFinishedHybridPartitionShouldBeUnknown);

    final CheckpointCoordinator checkpointCoordinator =
            newExecutionGraph.getCheckpointCoordinator();

    if (checkpointCoordinator != null) {
        // check whether we find a valid checkpoint
        if (!checkpointCoordinator.restoreInitialCheckpointIfPresent(
                new HashSet<>(newExecutionGraph.getAllVertices().values()))) {

            // check whether we can restore from a savepoint
            tryRestoreExecutionGraphFromSavepoint(
                    newExecutionGraph, jobGraph.getSavepointRestoreSettings());
        }
    }

    return newExecutionGraph;
}
```

DefaultExecutionGraphBuilder.buildGraph代码太多 重点看 executionGraph.attachJobGraph(sortedTopology); 核心转换相关概念

·JobVertex:实际上就是JobGraph的节点，代表一个或者一组 Operator实例，JobGraph仅是一个计算逻辑的描述，节点和节点之间通过Intermediate Data Set连接。

·ExecutionJobVertex:ExecutionGraph的Job节点，和JobGraph中 JobVertex一一对应，ExecutionJobVertex相当于一系列并行的操作。

·ExecutionVertex:ExecutionGraph中的子节点，代表 ExecutionJobVertex中的并行实例，在ExecutionVertex中 ExecutionVertexID作为唯一ID，每个ExecutionVertex都具备Execution变 量，Execution负责向TaskExecutor中提交和运行相应的Task。

·IntermediateResult:ExecutionJobVertex上游算子的中间数据集， 每个IntermediateResult包含多个IntermediateResultPartition，通过 IntermediateResultPartition生成物理执行图中的ResultPartition组件，用于 网络栈中上游Task节点的数据输出。

·Execution:ExecutionVertex节点中对应的执行单元， ExecutionVertex可以被执行多次，如recovery、re-computation和re- configuration等操作都会导致ExecutionVertex重新启动和执行，此时就会 通过Execution记录每次执行的操作，Execution提供了向TaskExecutor提 交Task的方法。

Task重启策略主要分为三种类型:固定延时重启(fixed- delay)、按失败率重启(failure-rate)以及无重启(none)。

·固定延时重启:按照restart-strategy.fixed-delay.delay参数给出的固 定间隔重启Job，如果重启次数达到fixed-delay.attempt配置值仍没有重启 成功，则停止重启。

·按失败率重启:按照restart-strategy.failure-rate.delay参数给出的固 定间隔重启Job，如果重启次数在failure-rate-interval参数规定的时间周期 内到达max-failures-per-interval配置的阈值仍没有成功，则停止重启。

·无重启:如果Job出现意外停止，则直接重启失败不再重启。

Flink中通过RestartStrategy接口表示 ExecutionGraph重启的策略配置，RestartStrategy接口主要实现类有 NoRestartStrategy、FailureRateRestartStrategy以及 FixeddelayRestartStrategy

RestartStrategy是通过 RestartStrategyFactory创建的，在RestartStrategyFactory基本实 现类中提供了创建RestartStrategy的抽象方法并通过子类实现。

需要注意的是， NoOrFixedIfCheckpointingEnabledRestartStrategyFactory会根据 Checkpoint是否开启，选择创建FixeddelayRestartStrategy还是 NoRestartStrategy。