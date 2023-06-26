JobManager的启动与初始化过程主要分为两部分:第一部分为启 动JobManagerRunner，第二部分为根据JobManagerRunner启动 JobMaster的RPC服务并创建和初始化JobMaster中的内部服务

·JobManagerRunner的默认实现为JobManagerRunnerImpl，且 JobManagerRunnerImpl实现了LeaderContender接口，因此 JobManagerRunner可以参与Leadership的竞争，实现JobManagerRunner高可用。JobManagerRunner还实现了OnCompletionActions接口，主要执行 Job处于终止状态的后续操作。

·JobManagerRunner包含JobMasterService的成员变量，而 JobMasterService接口的默认实现为JobMaster。通过JobManagerRunner中 JobMasterServiceFactory的默认实现类DefaultJobMasterServiceFactory可以 创建JobMasterService实例。

·JobMaster为JobManager的底层实现，JobManagerRunner和 JobMaster合并起来就是完整的JobManager功能实现。JobMaster不仅实现 了JobMasterService接口，还通过继承FencedRpcEndpoint基本实现类成为 RPC服务。此外，JobMaster通过实现JobMasterGateway接口，向集群其 他组件提供了RPC服务访问的能力，实现与其他组件之间的RPC通信。

因为 JobManagerRunner服务实现了高可用，所以在启动它时，会通过调用 leaderElectionService启动LeaderContender实现类 JobManagerRunnerImpl。代码如下，这里this对象实际上就是 JobManagerRunnerImpl实例。

起动点 Dispatcher.runJob() 中调用jobManagerRunner.start();

```java
private void runJob(JobManagerRunner jobManagerRunner, ExecutionType executionType)
        throws Exception {
    jobManagerRunner.start();
    jobManagerRunnerRegistry.register(jobManagerRunner);
    ...
    }
```

JobMasterServiceLeadershipRunner.start()

```java
@Override
public void start() throws Exception {
    LOG.debug("Start leadership runner for job {}.", getJobID());
    leaderElection = leaderElectionService.createLeaderElection();
    leaderElection.startLeaderElection(this);
}
```

DefaultLeaderElection.startLeaderElection(this)

```java
@Override
public void startLeaderElection(LeaderContender contender) throws Exception {
    Preconditions.checkState(
            leaderContender == null, "There shouldn't be any LeaderContender registered, yet.");
    leaderContender = Preconditions.checkNotNull(contender);

    parentService.register(leaderContender);
}
```

parentService.register(leaderContender);多个实现 StandaloneLeaderElectionService DefaultLeaderElectionService EmbeddedLeaderElectionService看DefaultLeaderElectionService

```java
@Override
protected void register(LeaderContender contender) throws Exception {
    checkNotNull(contender, "Contender must not be null.");

    synchronized (lock) {
        Preconditions.checkState(
                leaderContender == null,
                "Only one LeaderContender is allowed to be registered to this service.");
        Preconditions.checkState(
                leaderElectionDriver != null,
                "The DefaultLeaderElectionService should have established a connection to the backend before it's started.");

        leaderContender = contender;

        LOG.info(
                "LeaderContender {} has been registered for {}.",
                contender.getDescription(),
                leaderElectionDriver);

        if (issuedLeaderSessionID != null) {
            // notifying the LeaderContender shouldn't happen in the contender's main thread
          //callable接口线程执行
            runInLeaderEventThread(
                    () -> notifyLeaderContenderOfLeadership(issuedLeaderSessionID));
        }
    }
}
```

notifyLeaderContenderOfLeadership(issuedLeaderSessionID)

```java
@GuardedBy("lock")
private void notifyLeaderContenderOfLeadership(UUID sessionID) {
    if (leaderContender == null) {
        LOG.debug(
                "The grant leadership notification for session ID {} is not forwarded because the DefaultLeaderElectionService ({}) has no contender registered.",
                sessionID,
                leaderElectionDriver);
        return;
    } else if (!sessionID.equals(issuedLeaderSessionID)) {
        LOG.debug(
                "An out-dated leadership-acquired event with session ID {} was triggered. The current leader session ID is {}. The event will be ignored.",
                sessionID,
                issuedLeaderSessionID);
        return;
    }

    Preconditions.checkState(
            confirmedLeaderInformation.isEmpty(),
            "The leadership should have been granted while not having the leadership acquired.");

    LOG.debug(
            "Granting leadership to contender {} with session ID {}.",
            leaderContender.getDescription(),
            issuedLeaderSessionID);

    leaderContender.grantLeadership(issuedLeaderSessionID);
}
```

leaderContender.grantLeadership(issuedLeaderSessionID); 多个实现 看JobMasterServiceLeadershipRunner

```java
@Override
public void grantLeadership(UUID leaderSessionID) {
    runIfStateRunning(
            () -> startJobMasterServiceProcessAsync(leaderSessionID),
            "starting a new JobMasterServiceProcess");
}
```

```java
@GuardedBy("lock")
private void startJobMasterServiceProcessAsync(UUID leaderSessionId) {
    sequentialOperation =
            sequentialOperation.thenRun(
                    () ->
                            runIfValidLeader(
                                    leaderSessionId,
                                    ThrowingRunnable.unchecked(
                                            () ->
                                                    verifyJobSchedulingStatusAndCreateJobMasterServiceProcess(
                                                            leaderSessionId)),
                                    "verify job scheduling status and create JobMasterServiceProcess"));

    handleAsyncOperationError(sequentialOperation, "Could not start the job manager.");
}
```

又是扔线程 verifyJobSchedulingStatusAndCreateJobMasterServiceProcess(leaderSessionId)

```java
@GuardedBy("lock")
private void verifyJobSchedulingStatusAndCreateJobMasterServiceProcess(UUID leaderSessionId)
        throws FlinkException {
    try {
        if (jobResultStore.hasJobResultEntry(getJobID())) {
            jobAlreadyDone(leaderSessionId);
        } else {
            createNewJobMasterServiceProcess(leaderSessionId);
        }
    } catch (IOException e) {
        throw new FlinkException(
                String.format(
                        "Could not retrieve the job scheduling status for job %s.", getJobID()),
                e);
    }
}
```

createNewJobMasterServiceProcess(leaderSessionId);

```java
@GuardedBy("lock")
private void createNewJobMasterServiceProcess(UUID leaderSessionId) throws FlinkException {
    Preconditions.checkState(jobMasterServiceProcess.closeAsync().isDone());

    LOG.info(
            "{} for job {} was granted leadership with leader id {}. Creating new {}.",
            getClass().getSimpleName(),
            getJobID(),
            leaderSessionId,
            JobMasterServiceProcess.class.getSimpleName());
//@GuardedBy("lock")
  //private JobMasterServiceProcess jobMasterServiceProcess=JobMasterServiceProcess.waitingForLeadership();
  //工厂 create jobMasterServiceProcess
    jobMasterServiceProcess = jobMasterServiceProcessFactory.create(leaderSessionId);

    forwardIfValidLeader(
            leaderSessionId,
            jobMasterServiceProcess.getJobMasterGatewayFuture(),
            jobMasterGatewayFuture,
            "JobMasterGatewayFuture from JobMasterServiceProcess");
    forwardResultFuture(leaderSessionId, jobMasterServiceProcess.getResultFuture());
    confirmLeadership(leaderSessionId, jobMasterServiceProcess.getLeaderAddressFuture());
}
```

jobMasterServiceProcess = jobMasterServiceProcessFactory.create(leaderSessionId); 默认DefaultJobMasterServiceProcessFactory

```java
@Override
public JobMasterServiceProcess create(UUID leaderSessionId) {
    return new DefaultJobMasterServiceProcess(
            jobId,
            leaderSessionId,
            jobMasterServiceFactory,
            cause -> createArchivedExecutionGraph(JobStatus.FAILED, cause));
}
```

createArchivedExecutionGraph(JobStatus.FAILED, cause) 这里是 转换ExecutionGraph?

```java
@Override
public ArchivedExecutionGraph createArchivedExecutionGraph(
        JobStatus jobStatus, @Nullable Throwable cause) {
    return ArchivedExecutionGraph.createSparseArchivedExecutionGraph(
            jobId, jobName, jobStatus, cause, checkpointingSettings, initializationTimestamp);
}
```

ArchivedExecutionGraph.createSparseArchivedExecutionGraph()

```java
/**
 * Create a sparse ArchivedExecutionGraph for a job. Most fields will be empty, only job status
 * and error-related fields are set.
 还没转换?  Most fields will be empty
 */
public static ArchivedExecutionGraph createSparseArchivedExecutionGraph(
        JobID jobId,
        String jobName,
        JobStatus jobStatus,
        @Nullable Throwable throwable,
        @Nullable JobCheckpointingSettings checkpointingSettings,
        long initializationTimestamp) {
    return createSparseArchivedExecutionGraph(
            jobId,
            jobName,
            jobStatus,
            Collections.emptyMap(),
            Collections.emptyList(),
            throwable,
            checkpointingSettings,
            initializationTimestamp);
}
```

一时没找到 JobMaster初始化和启动 找下构造方法调用发现起始点在 工厂类创建jobMasterServiceProcess的时候创建了JobMaster

```java
jobMasterServiceProcess = jobMasterServiceProcessFactory.create(leaderSessionId);
```

new DefaultJobMasterServiceProcess()

```java
public DefaultJobMasterServiceProcess(
        JobID jobId,
        UUID leaderSessionId,
        JobMasterServiceFactory jobMasterServiceFactory,
        Function<Throwable, ArchivedExecutionGraph> failedArchivedExecutionGraphFactory) {
    this.jobId = jobId;
    this.leaderSessionId = leaderSessionId;
    this.jobMasterServiceFuture =
            jobMasterServiceFactory.createJobMasterService(leaderSessionId, this);

    jobMasterServiceFuture.whenComplete(
            (jobMasterService, throwable) -> {
                if (throwable != null) {
                    final JobInitializationException jobInitializationException =
                            new JobInitializationException(
                                    jobId, "Could not start the JobMaster.", throwable);

                    LOG.debug(
                            "Initialization of the JobMasterService for job {} under leader id {} failed.",
                            jobId,
                            leaderSessionId,
                            jobInitializationException);

                    resultFuture.complete(
                            JobManagerRunnerResult.forInitializationFailure(
                                    new ExecutionGraphInfo(
                                            failedArchivedExecutionGraphFactory.apply(
                                                    jobInitializationException)),
                                    jobInitializationException));
                } else {
                    registerJobMasterServiceFutures(jobMasterService);
                }
            });
}
```

this.jobMasterServiceFuture = jobMasterServiceFactory.createJobMasterService(leaderSessionId, this);

```java
@Override
public CompletableFuture<JobMasterService> createJobMasterService(
        UUID leaderSessionId, OnCompletionActions onCompletionActions) {

    return CompletableFuture.supplyAsync(
            FunctionUtils.uncheckedSupplier(
                    () -> internalCreateJobMasterService(leaderSessionId, onCompletionActions)),
            executor);
}
```

internalCreateJobMasterService 可以看到JobMaster的初始化和启动

```java
private JobMasterService internalCreateJobMasterService(
        UUID leaderSessionId, OnCompletionActions onCompletionActions) throws Exception {

    final JobMaster jobMaster =
            new JobMaster(
                    rpcService,
                    JobMasterId.fromUuidOrNull(leaderSessionId),
                    jobMasterConfiguration,
                    ResourceID.generate(),
                    jobGraph,
                    haServices,
                    slotPoolServiceSchedulerFactory,
                    jobManagerSharedServices,
                    heartbeatServices,
                    jobManagerJobMetricGroupFactory,
                    onCompletionActions,
                    fatalErrorHandler,
                    userCodeClassloader,
                    shuffleMaster,
                    lookup ->
                            new JobMasterPartitionTrackerImpl(
                                    jobGraph.getJobID(), shuffleMaster, lookup),
                    new DefaultExecutionDeploymentTracker(),
                    DefaultExecutionDeploymentReconciler::new,
                    BlocklistUtils.loadBlocklistHandlerFactory(
                            jobMasterConfiguration.getConfiguration()),
                    failureEnrichers,
                    initializationTimestamp);

    jobMaster.start();

    return jobMaster;
}
```