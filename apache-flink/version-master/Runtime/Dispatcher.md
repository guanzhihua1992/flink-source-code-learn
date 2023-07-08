功能:负责对集群中的作业进行接收和分发处理操作，客 户端可以通过与Dispatcher建立RPC连接，将作业过ClusterClient提交到集群Dispatcher服务中。Dispatcher通过JobGraph对象启动JobManager服务。

创建 以SessionDispatcherLeaderProcess.createDispatcher()为例

```java
private void createDispatcher(
        Collection<JobGraph> jobGraphs, Collection<JobResult> recoveredDirtyJobResults) {

    final DispatcherGatewayService dispatcherService =
            dispatcherGatewayServiceFactory.create(
                    DispatcherId.fromUuid(getLeaderSessionId()),
                    jobGraphs,
                    recoveredDirtyJobResults,
                    jobGraphStore,
                    jobResultStore);

    completeDispatcherSetup(dispatcherService);
}
```

```java
final void completeDispatcherSetup(DispatcherGatewayService dispatcherService) {
    runIfStateIs(State.RUNNING, () -> completeDispatcherSetupInternal(dispatcherService));
}
```

```java
private void completeDispatcherSetupInternal(
        DispatcherGatewayService createdDispatcherService) {
    Preconditions.checkState(
            dispatcherService == null, "The DispatcherGatewayService can only be set once.");
    dispatcherService = createdDispatcherService;
    dispatcherGatewayFuture.complete(createdDispatcherService.getGateway());
    FutureUtils.forward(createdDispatcherService.getShutDownFuture(), shutDownFuture);
    handleUnexpectedDispatcherServiceTermination(createdDispatcherService);
}
```

```java
/** Factory for {@link DispatcherGatewayService}. DispatcherGatewayServiceFactory 有两个实现类 默认实现类DefaultDispatcherGatewayServiceFactory和 ApplicationDispatcherGatewayServiceFactory*/
public interface DispatcherGatewayServiceFactory {
    DispatcherGatewayService create(
            DispatcherId dispatcherId,
            Collection<JobGraph> recoveredJobs,
            Collection<JobResult> recoveredDirtyJobResults,
            JobGraphWriter jobGraphWriter,
            JobResultStore jobResultStore);
}

/**
 * A {@link
 * org.apache.flink.runtime.dispatcher.runner.AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory
 * DispatcherGatewayServiceFactory} used when executing a job in Application Mode, i.e. the user's
 * main is executed on the same machine as the {@link Dispatcher} and the lifecycle of the cluster
 * is the same as the one of the application.
 *
 * <p>It instantiates a {@link
 * org.apache.flink.runtime.dispatcher.runner.AbstractDispatcherLeaderProcess.DispatcherGatewayService
 * DispatcherGatewayService} with an {@link ApplicationDispatcherBootstrap} containing the user's
 * program.
 *ApplicationDispatcherGatewayServiceFactory Application Mode 时候运用 单机版
 */
@Internal
public class ApplicationDispatcherGatewayServiceFactory
        implements AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory {
  
}
```

DefaultDispatcherGatewayServiceFactory.create()

```java
@Override
public AbstractDispatcherLeaderProcess.DispatcherGatewayService create(
        DispatcherId fencingToken,
        Collection<JobGraph> recoveredJobs,
        Collection<JobResult> recoveredDirtyJobResults,
        JobGraphWriter jobGraphWriter,
        JobResultStore jobResultStore) {

    final Dispatcher dispatcher;
    try {//创建  DispatcherFactory 分 job session 两类实现
        dispatcher =
                dispatcherFactory.createDispatcher(
                        rpcService,
                        fencingToken,
                        recoveredJobs,
                        recoveredDirtyJobResults,
                        (dispatcherGateway, scheduledExecutor, errorHandler) ->
                                new NoOpDispatcherBootstrap(),
                        PartialDispatcherServicesWithJobPersistenceComponents.from(
                                partialDispatcherServices, jobGraphWriter, jobResultStore));
    } catch (Exception e) {
        throw new FlinkRuntimeException("Could not create the Dispatcher rpc endpoint.", e);
    }
//启动
    dispatcher.start();
//包装代理
    return DefaultDispatcherGatewayService.from(dispatcher);
}
```

```java
/** {@link DispatcherFactory} which creates a {@link StandaloneDispatcher}. */
//关注下这个工厂类是个枚举类 单例模式的最好实现方式
public enum SessionDispatcherFactory implements DispatcherFactory {
    INSTANCE;

    @Override
    public StandaloneDispatcher createDispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            Collection<JobGraph> recoveredJobs,
            Collection<JobResult> recoveredDirtyJobResults,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            PartialDispatcherServicesWithJobPersistenceComponents
                    partialDispatcherServicesWithJobPersistenceComponents)
            throws Exception {
        // create the default dispatcher
        return new StandaloneDispatcher(
                rpcService,
                fencingToken,
                recoveredJobs,
                recoveredDirtyJobResults,
                dispatcherBootstrapFactory,
                DispatcherServices.from(
                        partialDispatcherServicesWithJobPersistenceComponents,
                        JobMasterServiceLeadershipRunnerFactory.INSTANCE,
                        CheckpointResourcesCleanupRunnerFactory.INSTANCE));
    }
}
```

找下单例在哪里出来的 DefaultDispatcherResourceManagerComponentFactory

```java
public static DefaultDispatcherResourceManagerComponentFactory createSessionComponentFactory(
        ResourceManagerFactory<?> resourceManagerFactory) {
    return new DefaultDispatcherResourceManagerComponentFactory(
            DefaultDispatcherRunnerFactory.createSessionRunner(
                    SessionDispatcherFactory.INSTANCE),
            resourceManagerFactory,
            SessionRestEndpointFactory.INSTANCE);
}
```

StandaloneDispatcher new方法直接super了 Dispatcher 其他构造函数都是私有 只有一个protected 且@VisibleForTesting

```java
@VisibleForTesting
protected Dispatcher(
        RpcService rpcService,
        DispatcherId fencingToken,
        Collection<JobGraph> recoveredJobs,
        Collection<JobResult> recoveredDirtyJobs,
        DispatcherBootstrapFactory dispatcherBootstrapFactory,
        DispatcherServices dispatcherServices,
        JobManagerRunnerRegistry jobManagerRunnerRegistry,
        ResourceCleanerFactory resourceCleanerFactory)
        throws Exception {
    super(rpcService, RpcServiceUtils.createRandomName(DISPATCHER_NAME), fencingToken);
    assertRecoveredJobsAndDirtyJobResults(recoveredJobs, recoveredDirtyJobs);
//从dispatcherServices 拿了一堆配置和各种服务
    this.configuration = dispatcherServices.getConfiguration();
    this.highAvailabilityServices = dispatcherServices.getHighAvailabilityServices();
    this.resourceManagerGatewayRetriever =
            dispatcherServices.getResourceManagerGatewayRetriever();
    this.heartbeatServices = dispatcherServices.getHeartbeatServices();
    this.blobServer = dispatcherServices.getBlobServer();
    this.fatalErrorHandler = dispatcherServices.getFatalErrorHandler();
    this.failureEnrichers = dispatcherServices.getFailureEnrichers();
    this.jobGraphWriter = dispatcherServices.getJobGraphWriter();
    this.jobResultStore = dispatcherServices.getJobResultStore();
    this.jobManagerMetricGroup = dispatcherServices.getJobManagerMetricGroup();
    this.metricServiceQueryAddress = dispatcherServices.getMetricQueryServiceAddress();
    this.ioExecutor = dispatcherServices.getIoExecutor();

    this.jobManagerSharedServices =
            JobManagerSharedServices.fromConfiguration(
                    configuration, blobServer, fatalErrorHandler);

    this.jobManagerRunnerRegistry =
            new OnMainThreadJobManagerRunnerRegistry(
                    jobManagerRunnerRegistry, this.getMainThreadExecutor());

    this.historyServerArchivist = dispatcherServices.getHistoryServerArchivist();

    this.executionGraphInfoStore = dispatcherServices.getArchivedExecutionGraphStore();

    this.jobManagerRunnerFactory = dispatcherServices.getJobManagerRunnerFactory();
    this.cleanupRunnerFactory = dispatcherServices.getCleanupRunnerFactory();

    this.jobManagerRunnerTerminationFutures =
            new HashMap<>(INITIAL_JOB_MANAGER_RUNNER_REGISTRY_CAPACITY);

    this.shutDownFuture = new CompletableFuture<>();

    this.dispatcherBootstrapFactory = checkNotNull(dispatcherBootstrapFactory);

    this.recoveredJobs = new HashSet<>(recoveredJobs);

    this.recoveredDirtyJobs = new HashSet<>(recoveredDirtyJobs);

    this.blobServer.retainJobs(
            recoveredJobs.stream().map(JobGraph::getJobID).collect(Collectors.toSet()),
            dispatcherServices.getIoExecutor());

    this.dispatcherCachedOperationsHandler =
            new DispatcherCachedOperationsHandler(
                    dispatcherServices.getOperationCaches(),
                    this::triggerCheckpointAndGetCheckpointID,
                    this::triggerSavepointAndGetLocation,
                    this::stopWithSavepointAndGetLocation);

    this.localResourceCleaner =
            resourceCleanerFactory.createLocalResourceCleaner(this.getMainThreadExecutor());
    this.globalResourceCleaner =
            resourceCleanerFactory.createGlobalResourceCleaner(this.getMainThreadExecutor());

    this.webTimeout = Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT));

    this.jobClientAlivenessCheckInterval =
            configuration.get(CLIENT_ALIVENESS_CHECK_DURATION).toMillis();
}
```

看下 super(rpcService, RpcServiceUtils.createRandomName(DISPATCHER_NAME), fencingToken);

```java
protected FencedRpcEndpoint(RpcService rpcService, String endpointId, F fencingToken) {
    super(rpcService, endpointId);

    Preconditions.checkNotNull(fencingToken, "The fence token should be null");
    Preconditions.checkNotNull(rpcServer, "The rpc server should be null");

    this.fencingToken = fencingToken;
}

protected RpcEndpoint(final RpcService rpcService, final String endpointId) {
        this.rpcService = checkNotNull(rpcService, "rpcService");
        this.endpointId = checkNotNull(endpointId, "endpointId");
//rpcService.startServer(this) rpc服务启动  这个在rpc相关细看 就是通过 actorSystem 当前dispatcher作为一个actor actor 可以与其他组件通信 而且akka actor之间调用
        this.rpcServer = rpcService.startServer(this);
        this.resourceRegistry = new CloseableRegistry();
//主线程执行器
        this.mainThreadExecutor =
                new MainThreadExecutor(rpcServer, this::validateRunsInMainThread, endpointId);
        //注册  RpcEndpoint
  registerResource(this.mainThreadExecutor);
    }
```

dispatcher.start();

```java
public final void start() {
    rpcServer.start();
}

@Override
    public void start() {
        rpcEndpoint.tell(ControlMessages.START, ActorRef.noSender());//扔了一个消息出去
    }
```

DefaultDispatcherGatewayService.from(dispatcher);  包装代理一把

```java
public static DefaultDispatcherGatewayService from(Dispatcher dispatcher) {
    return new DefaultDispatcherGatewayService(dispatcher);
}
```

```java
private DefaultDispatcherGatewayService(Dispatcher dispatcher) {
    this.dispatcher = dispatcher;
    this.dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);
}
```

工作 最核心就是 submitJob 提交job到集群 以及关联的沟通操作 都是通过rpc

```java
@Override
public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
    log.info(
            "Received JobGraph submission '{}' ({}).", jobGraph.getName(), jobGraph.getJobID());

    try {
        if (isDuplicateJob(jobGraph.getJobID())) {
            if (isInGloballyTerminalState(jobGraph.getJobID())) {
                log.warn(
                        "Ignoring JobGraph submission '{}' ({}) because the job already reached a globally-terminal state (i.e. {}) in a previous execution.",
                        jobGraph.getName(),
                        jobGraph.getJobID(),
                        Arrays.stream(JobStatus.values())
                                .filter(JobStatus::isGloballyTerminalState)
                                .map(JobStatus::name)
                                .collect(Collectors.joining(", ")));
            }

            final DuplicateJobSubmissionException exception =
                    isInGloballyTerminalState(jobGraph.getJobID())
                            ? DuplicateJobSubmissionException.ofGloballyTerminated(
                                    jobGraph.getJobID())
                            : DuplicateJobSubmissionException.of(jobGraph.getJobID());
            return FutureUtils.completedExceptionally(exception);
        } else if (isPartialResourceConfigured(jobGraph)) {
            return FutureUtils.completedExceptionally(
                    new JobSubmissionException(
                            jobGraph.getJobID(),
                            "Currently jobs is not supported if parts of the vertices have "
                                    + "resources configured. The limitation will be removed in future versions."));
        } else {
            return internalSubmitJob(jobGraph);
        }
    } catch (FlinkException e) {
        return FutureUtils.completedExceptionally(e);
    }
}
```

重点 internalSubmitJob(jobGraph);

```java
private CompletableFuture<Acknowledge> internalSubmitJob(JobGraph jobGraph) {
  //并行度参数
    applyParallelismOverrides(jobGraph);
    log.info("Submitting job '{}' ({}).", jobGraph.getName(), jobGraph.getJobID());
    return waitForTerminatingJob(jobGraph.getJobID(), jobGraph, this::persistAndRunJob)
            .handle((ignored, throwable) -> handleTermination(jobGraph.getJobID(), throwable))
            .thenCompose(Function.identity());
}
```

this::persistAndRunJob

```java
private void persistAndRunJob(JobGraph jobGraph) throws Exception {
    jobGraphWriter.putJobGraph(jobGraph);
    initJobClientExpiredTime(jobGraph);
    runJob(createJobMasterRunner(jobGraph), ExecutionType.SUBMISSION);
}
```

```java
private JobManagerRunner createJobMasterRunner(JobGraph jobGraph) throws Exception {
    Preconditions.checkState(!jobManagerRunnerRegistry.isRegistered(jobGraph.getJobID()));
    return jobManagerRunnerFactory.createJobManagerRunner(
            jobGraph,
            configuration,
            getRpcService(),
            highAvailabilityServices,
            heartbeatServices,
            jobManagerSharedServices,
            new DefaultJobManagerJobMetricGroupFactory(jobManagerMetricGroup),
            fatalErrorHandler,
            failureEnrichers,
            System.currentTimeMillis());
}
/** Interface for a runner which executes a {@link JobMaster}. 管理JobMaster高可用*/
public interface JobManagerRunner extends AutoCloseableAsync {}
```

```java
private void runJob(JobManagerRunner jobManagerRunner, ExecutionType executionType)
        throws Exception {
  //jobManagerRunner 启动 注册
    jobManagerRunner.start();
    jobManagerRunnerRegistry.register(jobManagerRunner);

    final JobID jobId = jobManagerRunner.getJobID();

    final CompletableFuture<CleanupJobState> cleanupJobStateFuture =
            jobManagerRunner
                    .getResultFuture()
                    .handleAsync(
                            (jobManagerRunnerResult, throwable) -> {
                                Preconditions.checkState(
                                        jobManagerRunnerRegistry.isRegistered(jobId)
                                                && jobManagerRunnerRegistry.get(jobId)
                                                        == jobManagerRunner,
                                        "The job entry in runningJobs must be bound to the lifetime of the JobManagerRunner.");

                                if (jobManagerRunnerResult != null) {
                                    return handleJobManagerRunnerResult(
                                            jobManagerRunnerResult, executionType);
                                } else {
                                    return CompletableFuture.completedFuture(
                                            jobManagerRunnerFailed(
                                                    jobId, JobStatus.FAILED, throwable));
                                }
                            },
                            getMainThreadExecutor())
                    .thenCompose(Function.identity());

    final CompletableFuture<Void> jobTerminationFuture =
            cleanupJobStateFuture.thenCompose(
                    cleanupJobState ->
                            removeJob(jobId, cleanupJobState)
                                    .exceptionally(
                                            throwable ->
                                                    logCleanupErrorWarning(jobId, throwable)));

    FutureUtils.handleUncaughtException(
            jobTerminationFuture,
            (thread, throwable) -> fatalErrorHandler.onFatalError(throwable));
    registerJobManagerRunnerTerminationFuture(jobId, jobTerminationFuture);
}
```