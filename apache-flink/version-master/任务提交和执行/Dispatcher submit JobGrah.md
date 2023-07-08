

1)ClusterClient调用Dispatcher.submit(JobGraph)的RPC方 法，将创建好的JobGraph提交至集群运行时。

2)在集群运行时中调用Dispatcher.internalSubmitJob()方法执 行JobGraph，然后调用waitForTerminatingJobManager()方法获取 jobManagerTerminationFuture对象。

3)在jobManagerTerminationFuture中组合异步方法 persistAndRunJob()，persistAndRunJob()方法主要涉及JobGraph的 持久化及执行操作。

4)调用Dispatcher.runJob()方法，执行JobGraph并返回 runJobFuture对象，此时runJob()方法主要涵盖jobManagerRunner的 异步启动过程。然后调用createJobManagerRunner()方法，将 jobManagerRunnerFactory.createJobManagerRunner()操作放置在 CompletableFuture中。

5)在返回的CompletableFuture对象中，调用thenApply方法来增 加对startJobManagerRunner()方法的调用，实际上就是启动 JobManagerRunner组件。

6)调用JobManagerRunner.start()方法正式启动 JobManagerRunner。因为JobManagerRunner是实现高可用的，所以需 要借助leaderElectionService.start(this)方法启动 JobManagerRunner组件。JobManagerRunner启动完毕后，接下来会调 用JobManagerRunnerImpl.grantLeadership()方法，授予当前 JobManagerRunner领导权，完成对JobMaster服务的启动。

7)在JobManagerRunnerImpl.grantLeadership()方法中调用 verifyJobSchedulingStatusAndStartJobManager()方法获取 JobSchedulingStatus状态，判断当前的Job是否启动过。如果获取的 状态是JobSchedulingStatus.DONE，则调用jobAlreadyDone()方法完 成后续处理;否则，调用startJobMaster(leaderSessionId)方法创建 并启动JobMaster。

8)在JobManagerRunnerImpl.startJobMaster()中调用 jobMasterService.start(new JobMasterId(leaderSessionId))方法 启动JobMaster，此时会返回startFuture对象。同时在start()方法中 会调用内部的start()方法启动JobMaster RPC服务，此时JobMaster的 RPC服务启动完成并对外提供服务。

9)在JobMaster启动完毕后，下一步是对JobGraph中的作业进行 调度，主要会调用startJobExecution()方法开始JobGraph的调度和运 行。作业如果启动成功，会将Acknowledge信息返回给Dispatcher服 务。

10)Dispatcher将Acknowledge信息返回给ClusterClient，该信 息最终会被返回给客户端。

```java
// ------------------------------------------------------
// RPCs
// ------------------------------------------------------

@Override
public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
    log.info(
            "Received JobGraph submission '{}' ({}).", jobGraph.getName(), jobGraph.getJobID());

    try {//判断提交的JobGraph中JobID是否重复，如果重复则抛出 DuplicateJobSubmission-Exception
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
          // JobGraph中不支持部分资源配置，因此要么全部节点都配置资源，要么全不配置
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

internalSubmitJob(jobGraph)

```java
private CompletableFuture<Acknowledge> internalSubmitJob(JobGraph jobGraph) {
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
  //调用jobGraphWriter.putJobGraph()方法对JobGraph进行持久 化，如果基于ZooKeeper实现了集群高可用，则将JobGraph记录到 ZooKeeperJobGraphStore中，异常情况下会通过 ZooKeeperJobGraphStore恢复作业。
    jobGraphWriter.putJobGraph(jobGraph);
  //以initialClientHeartbeatTimeout
    initJobClientExpiredTime(jobGraph);
  //调用runJob(jobGraph)方法执行jobGraph
    runJob(createJobMasterRunner(jobGraph), ExecutionType.SUBMISSION);
}
```

runJob(createJobMasterRunner(jobGraph), ExecutionType.SUBMISSION);

```java
private void runJob(JobManagerRunner jobManagerRunner, ExecutionType executionType)
        throws Exception {
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

回头看 createJobMasterRunner(jobGraph)

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
```

jobManagerRunner.start(); JobMasterServiceLeadershipRunner实现类

```java
@Override
public void start() throws Exception {
    LOG.debug("Start leadership runner for job {}.", getJobID());
    leaderElection = leaderElectionService.createLeaderElection();
    leaderElection.startLeaderElection(this);
}
```