功能: 负责启动和管理Dispatcher组件，并支持对 Dispatcher组件的Leader选举。当Dispatcher集群组件出现异常并停止 时，会通过DispatcherRunner重新选择和启动新的Dispatcher服务，从而 保证Dispatcher组件的高可用。

创建DispatcherRunner是一个接口,有一个默认实现类DefaultDispatcherRunner.ClusterEntrypoint.runCluster().

```java
private void runCluster(Configuration configuration, PluginManager pluginManager)
        throws Exception {
    synchronized (lock) {
        initializeServices(configuration, pluginManager);

        // write host information into configuration
        configuration.setString(JobManagerOptions.ADDRESS, commonRpcService.getAddress());
        configuration.setInteger(JobManagerOptions.PORT, commonRpcService.getPort());

        final DispatcherResourceManagerComponentFactory
                dispatcherResourceManagerComponentFactory =
                        createDispatcherResourceManagerComponentFactory(configuration);

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

DispatcherResourceManagerComponentFactory.create()

```java
@Override
public DispatcherResourceManagerComponent create(
        Configuration configuration,
        ResourceID resourceId,
        Executor ioExecutor,
        RpcService rpcService,
        HighAvailabilityServices highAvailabilityServices,
        BlobServer blobServer,
        HeartbeatServices heartbeatServices,
        DelegationTokenManager delegationTokenManager,
        MetricRegistry metricRegistry,
        ExecutionGraphInfoStore executionGraphInfoStore,
        MetricQueryServiceRetriever metricQueryServiceRetriever,
        Collection<FailureEnricher> failureEnrichers,
        FatalErrorHandler fatalErrorHandler)
        throws Exception {

    LeaderRetrievalService dispatcherLeaderRetrievalService = null;
    LeaderRetrievalService resourceManagerRetrievalService = null;
    WebMonitorEndpoint<?> webMonitorEndpoint = null;
    ResourceManagerService resourceManagerService = null;
    DispatcherRunner dispatcherRunner = null;

    try {
        dispatcherLeaderRetrievalService =
                highAvailabilityServices.getDispatcherLeaderRetriever();

        resourceManagerRetrievalService =
                highAvailabilityServices.getResourceManagerLeaderRetriever();

        final LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever =
                new RpcGatewayRetriever<>(
                        rpcService,
                        DispatcherGateway.class,
                        DispatcherId::fromUuid,
                        new ExponentialBackoffRetryStrategy(
                                12, Duration.ofMillis(10), Duration.ofMillis(50)));

        final LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever =
                new RpcGatewayRetriever<>(
                        rpcService,
                        ResourceManagerGateway.class,
                        ResourceManagerId::fromUuid,
                        new ExponentialBackoffRetryStrategy(
                                12, Duration.ofMillis(10), Duration.ofMillis(50)));

        final ScheduledExecutorService executor =
                WebMonitorEndpoint.createExecutorService(
                        configuration.getInteger(RestOptions.SERVER_NUM_THREADS),
                        configuration.getInteger(RestOptions.SERVER_THREAD_PRIORITY),
                        "DispatcherRestEndpoint");

        final long updateInterval =
                configuration.getLong(MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL);
        final MetricFetcher metricFetcher =
                updateInterval == 0
                        ? VoidMetricFetcher.INSTANCE
                        : MetricFetcherImpl.fromConfiguration(
                                configuration,
                                metricQueryServiceRetriever,
                                dispatcherGatewayRetriever,
                                executor);

        webMonitorEndpoint =
                restEndpointFactory.createRestEndpoint(
                        configuration,
                        dispatcherGatewayRetriever,
                        resourceManagerGatewayRetriever,
                        blobServer,
                        executor,
                        metricFetcher,
                        highAvailabilityServices.getClusterRestEndpointLeaderElectionService(),
                        fatalErrorHandler);

        log.debug("Starting Dispatcher REST endpoint.");
        webMonitorEndpoint.start();

        final String hostname = RpcUtils.getHostname(rpcService);

        resourceManagerService =
                ResourceManagerServiceImpl.create(
                        resourceManagerFactory,
                        configuration,
                        resourceId,
                        rpcService,
                        highAvailabilityServices,
                        heartbeatServices,
                        delegationTokenManager,
                        fatalErrorHandler,
                        new ClusterInformation(hostname, blobServer.getPort()),
                        webMonitorEndpoint.getRestBaseUrl(),
                        metricRegistry,
                        hostname,
                        ioExecutor);

        final HistoryServerArchivist historyServerArchivist =
                HistoryServerArchivist.createHistoryServerArchivist(
                        configuration, webMonitorEndpoint, ioExecutor);

        final DispatcherOperationCaches dispatcherOperationCaches =
                new DispatcherOperationCaches(
                        configuration.get(RestOptions.ASYNC_OPERATION_STORE_DURATION));

        final PartialDispatcherServices partialDispatcherServices =
                new PartialDispatcherServices(
                        configuration,
                        highAvailabilityServices,
                        resourceManagerGatewayRetriever,
                        blobServer,
                        heartbeatServices,
                        () ->
                                JobManagerMetricGroup.createJobManagerMetricGroup(
                                        metricRegistry, hostname),
                        executionGraphInfoStore,
                        fatalErrorHandler,
                        historyServerArchivist,
                        metricRegistry.getMetricQueryServiceGatewayRpcAddress(),
                        ioExecutor,
                        dispatcherOperationCaches,
                        failureEnrichers);

        log.debug("Starting Dispatcher.");
        dispatcherRunner =
                dispatcherRunnerFactory.createDispatcherRunner(
                        highAvailabilityServices.getDispatcherLeaderElectionService(),
                        fatalErrorHandler,
                        new HaServicesJobPersistenceComponentFactory(highAvailabilityServices),
                        ioExecutor,
                        rpcService,
                        partialDispatcherServices);

        log.debug("Starting ResourceManagerService.");
        resourceManagerService.start();

        resourceManagerRetrievalService.start(resourceManagerGatewayRetriever);
        dispatcherLeaderRetrievalService.start(dispatcherGatewayRetriever);

        return new DispatcherResourceManagerComponent(
                dispatcherRunner,
                resourceManagerService,
                dispatcherLeaderRetrievalService,
                resourceManagerRetrievalService,
                webMonitorEndpoint,
                fatalErrorHandler,
                dispatcherOperationCaches);

    } catch (Exception exception) {
        // clean up all started components
        if (dispatcherLeaderRetrievalService != null) {
            try {
                dispatcherLeaderRetrievalService.stop();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }
        }

        if (resourceManagerRetrievalService != null) {
            try {
                resourceManagerRetrievalService.stop();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }
        }

        final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);

        if (webMonitorEndpoint != null) {
            terminationFutures.add(webMonitorEndpoint.closeAsync());
        }

        if (resourceManagerService != null) {
            terminationFutures.add(resourceManagerService.closeAsync());
        }

        if (dispatcherRunner != null) {
            terminationFutures.add(dispatcherRunner.closeAsync());
        }

        final FutureUtils.ConjunctFuture<Void> terminationFuture =
                FutureUtils.completeAll(terminationFutures);

        try {
            terminationFuture.get();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        throw new FlinkException(
                "Could not create the DispatcherResourceManagerComponent.", exception);
    }
}
```

dispatcherRunnerFactory.createDispatcherRunner()

```java
@Override
public DispatcherRunner createDispatcherRunner(
        LeaderElectionService leaderElectionService,
        FatalErrorHandler fatalErrorHandler,
        JobPersistenceComponentFactory jobPersistenceComponentFactory,
        Executor ioExecutor,
        RpcService rpcService,
        PartialDispatcherServices partialDispatcherServices)
        throws Exception {

    final DispatcherLeaderProcessFactory dispatcherLeaderProcessFactory =
            //两层工厂
            dispatcherLeaderProcessFactoryFactory.createFactory(
                    jobPersistenceComponentFactory,
                    ioExecutor,
                    rpcService,
                    partialDispatcherServices,
                    fatalErrorHandler);

    return DefaultDispatcherRunner.create(
            leaderElectionService, fatalErrorHandler, dispatcherLeaderProcessFactory);
}
```

```java
public static DispatcherRunner create(
        LeaderElectionService leaderElectionService,
        FatalErrorHandler fatalErrorHandler,
        DispatcherLeaderProcessFactory dispatcherLeaderProcessFactory)
        throws Exception {
    final DefaultDispatcherRunner dispatcherRunner =
            new DefaultDispatcherRunner(
      //持有三个对象 leaderElectionService 高可用选举 fatalErrorHandler 错误处理 dispatcherLeaderProcessFactory 工厂
                    leaderElectionService, fatalErrorHandler, dispatcherLeaderProcessFactory);
    //启动
  dispatcherRunner.start();
    return dispatcherRunner;
}
```

启动dispatcherRunner.start();

```
void start() throws Exception {
    leaderElection = leaderElectionService.createLeaderElection();
    leaderElection.startLeaderElection(this);
}
```

leaderElection.startLeaderElection(this); 选主

```java
@Override
public void startLeaderElection(LeaderContender contender) throws Exception {
    Preconditions.checkState(
            leaderContender == null, "There shouldn't be any LeaderContender registered, yet.");
    leaderContender = Preconditions.checkNotNull(contender);
//就做了一个注册
    parentService.register(leaderContender);
}
```

parentService.register(leaderContender); AbstractLeaderElectionService 有三个实现类,看下默认实现DefaultLeaderElectionService

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
          //扔到 LeaderEventThread 线程执行等待结果 callable
            runInLeaderEventThread(
                    () -> notifyLeaderContenderOfLeadership(issuedLeaderSessionID));
        }
    }
}
```

notifyLeaderContenderOfLeadership(issuedLeaderSessionID)

```java
private void notifyLeaderContenderOfLeadership(UUID sessionID) {
  //判断检查
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

leaderContender.grantLeadership(issuedLeaderSessionID);

```java
@Override
public void grantLeadership(UUID leaderSessionID) {
  //又是一个线程 runable
    runActionIfRunning(
            () -> {
                LOG.info(
                        "{} was granted leadership with leader id {}. Creating new {}.",
                        getClass().getSimpleName(),
                        leaderSessionID,
                        DispatcherLeaderProcess.class.getSimpleName());
                startNewDispatcherLeaderProcess(leaderSessionID);
            });
}
```

startNewDispatcherLeaderProcess(leaderSessionID);

```java
private void startNewDispatcherLeaderProcess(UUID leaderSessionID) {
  //停旧
    stopDispatcherLeaderProcess();
//启新
    dispatcherLeaderProcess = createNewDispatcherLeaderProcess(leaderSessionID);

    final DispatcherLeaderProcess newDispatcherLeaderProcess = dispatcherLeaderProcess;
    FutureUtils.assertNoException(
            previousDispatcherLeaderProcessTerminationFuture.thenRun(
                    newDispatcherLeaderProcess::start));
}
```

newDispatcherLeaderProcess::start;DispatcherLeaderProcess 有 job session 实现类,SessionDispatcherLeaderProcess.onStart()

```java
@Override
protected void onStart() {
    startServices();

    onGoingRecoveryOperation =
            createDispatcherBasedOnRecoveredJobGraphsAndRecoveredDirtyJobResults();
}

private void startServices() {
        try {
          //只有一个 jobGraphStore start
            jobGraphStore.start(this);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Could not start %s when trying to start the %s.",
                            jobGraphStore.getClass().getSimpleName(), getClass().getSimpleName()),
                    e);
        }
    }
```

重点在 createDispatcherBasedOnRecoveredJobGraphsAndRecoveredDirtyJobResults();

```java
private CompletableFuture<Void>
        createDispatcherBasedOnRecoveredJobGraphsAndRecoveredDirtyJobResults() {
    final CompletableFuture<Collection<JobResult>> dirtyJobsFuture =
            CompletableFuture.supplyAsync(this::getDirtyJobResultsIfRunning, ioExecutor);

    return dirtyJobsFuture
            .thenApplyAsync(
                    dirtyJobs ->
                            this.recoverJobsIfRunning(
                                    dirtyJobs.stream()
                                            .map(JobResult::getJobId)
                                            .collect(Collectors.toSet())),
                    ioExecutor)
            .thenAcceptBoth(dirtyJobsFuture, this::createDispatcherIfRunning)
            .handle(this::onErrorIfRunning);
}
```

getDirtyJobResultsIfRunning取历史job结果数据 DirtyJobResults取名很有意思  

```java
private Collection<JobResult> getDirtyJobResults() {
    try {
        return jobResultStore.getDirtyResults();
    } catch (IOException e) {
        throw new FlinkRuntimeException(
                "Could not retrieve JobResults of globally-terminated jobs from JobResultStore",
                e);
    }
}
```

```java
@Override
public Set<JobResult> getDirtyResults() throws IOException {
  //withReadLock 加读锁
  //getDirtyResultsInternal 两个实现 embedded 和 file system
    return withReadLock(this::getDirtyResultsInternal);
}

@GuardedBy("readWriteLock")//锁
    protected abstract Set<JobResult> getDirtyResultsInternal() throws IOException;
```

recoverJobsIfRunning job ID 获取 job graph

```java
private Collection<JobGraph> recoverJobsIfRunning(Set<JobID> recoveredDirtyJobResults) {
    return supplyUnsynchronizedIfRunning(() -> recoverJobs(recoveredDirtyJobResults))
            .orElse(Collections.emptyList());
}

private Collection<JobGraph> recoverJobs(Set<JobID> recoveredDirtyJobResults) {
        log.info("Recover all persisted job graphs that are not finished, yet.");
        final Collection<JobID> jobIds = getJobIds();
        final Collection<JobGraph> recoveredJobGraphs = new ArrayList<>();

        for (JobID jobId : jobIds) {
            if (!recoveredDirtyJobResults.contains(jobId)) {
                tryRecoverJob(jobId).ifPresent(recoveredJobGraphs::add);
            } else {
                log.info(
                        "Skipping recovery of a job with job id {}, because it already reached a globally terminal state",
                        jobId);
            }
        }

        log.info("Successfully recovered {} persisted job graphs.", recoveredJobGraphs.size());

        return recoveredJobGraphs;
    }

    private Collection<JobID> getJobIds() {
        try {
            return jobGraphStore.getJobIds();
        } catch (Exception e) {
            throw new FlinkRuntimeException("Could not retrieve job ids of persisted jobs.", e);
        }
    }
```

createDispatcherIfRunning 创建job对应的 Dispatcher 并 runIfStateIs

```java
private void createDispatcherIfRunning(
        Collection<JobGraph> jobGraphs, Collection<JobResult> recoveredDirtyJobResults) {
    runIfStateIs(State.RUNNING, () -> createDispatcher(jobGraphs, recoveredDirtyJobResults));
}
```

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

runIfStateIs

```java
final void runIfStateIs(State expectedState, Runnable action) {
    runIfState(expectedState::equals, action);
}

private void runIfStateIsNot(State notExpectedState, Runnable action) {
    runIfState(state -> !notExpectedState.equals(state), action);
}

private void runIfState(Predicate<State> actionPredicate, Runnable action) {
    synchronized (lock) {
        if (actionPredicate.test(state)) {
            action.run();
        }
    }
}
```