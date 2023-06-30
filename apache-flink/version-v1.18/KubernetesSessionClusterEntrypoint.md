和Yarn资源管理器实现的Session集群相同， KubernetesSessionClusterEntrypoint作为基于Kubernetes资源管理 器实现的Session集群入口类，定义了Kubernetes Flink Session集群 的启动和初始化逻辑



```java
/** Entry point for a Kubernetes session cluster. Kubernetes session集群入口*/
public class KubernetesSessionClusterEntrypoint extends SessionClusterEntrypoint {

    public KubernetesSessionClusterEntrypoint(Configuration configuration) {
        super(configuration);
    }

    @Override
    protected DispatcherResourceManagerComponentFactory
            createDispatcherResourceManagerComponentFactory(Configuration configuration) {
        return DefaultDispatcherResourceManagerComponentFactory.createSessionComponentFactory(
                KubernetesResourceManagerFactory.getInstance());
    }

    public static void main(String[] args) {
        // startup checks and logging
      // 启动检查和日志设定
        EnvironmentInformation.logEnvironmentInfo(
                LOG, KubernetesSessionClusterEntrypoint.class.getSimpleName(), args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);
//获取动态参数
        final Configuration dynamicParameters =
                ClusterEntrypointUtils.parseParametersOrExit(
                        args,
                        new DynamicParametersConfigurationParserFactory(),
                        KubernetesSessionClusterEntrypoint.class);
        final ClusterEntrypoint entrypoint =
                new KubernetesSessionClusterEntrypoint(
                        KubernetesEntrypointUtils.loadConfiguration(dynamicParameters));
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

KubernetesSessionClusterEntrypoint.createDispatcherResourceManagerComponentFactory

```
@Override
protected DispatcherResourceManagerComponentFactory
        createDispatcherResourceManagerComponentFactory(Configuration configuration) {
    return DefaultDispatcherResourceManagerComponentFactory.createSessionComponentFactory(
            KubernetesResourceManagerFactory.getInstance());
}
```

dispatcherResourceManagerComponentFactory.create  DefaultDispatcherResourceManagerComponentFactory默认实现

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

highAvailabilityServices.getDispatcherLeaderRetriever();

highAvailabilityServices.getResourceManagerLeaderRetriever();

ResourceManagerServiceImpl.create

dispatcherRunnerFactory.createDispatcherRunner(

