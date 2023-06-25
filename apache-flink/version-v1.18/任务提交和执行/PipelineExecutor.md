将StreamGraph转换为JobGraph,PipelineExecutor主要通过 PipelineExecutorFactory创建，而PipelineExecutorFactory主要通 过Java Service Provider Interface(SPI)的方式加载

```java
@Internal
public JobClient executeAsync(StreamGraph streamGraph) throws Exception {
    checkNotNull(streamGraph, "StreamGraph cannot be null.");
    final PipelineExecutor executor = getPipelineExecutor();

    CompletableFuture<JobClient> jobClientFuture =
            executor.execute(streamGraph, configuration, userClassloader);

    try {
        JobClient jobClient = jobClientFuture.get();
        jobListeners.forEach(jobListener -> jobListener.onJobSubmitted(jobClient, null));
        collectIterators.forEach(iterator -> iterator.setJobClient(jobClient));
        collectIterators.clear();
        return jobClient;
    } catch (ExecutionException executionException) {
        final Throwable strippedException =
                ExceptionUtils.stripExecutionException(executionException);
        jobListeners.forEach(
                jobListener -> jobListener.onJobSubmitted(null, strippedException));

        throw new FlinkException(
                String.format("Failed to execute job '%s'.", streamGraph.getJobName()),
                strippedException);
    }
}
```

getPipelineExecutor()

```java
private PipelineExecutor getPipelineExecutor() throws Exception {
    checkNotNull(
            configuration.get(DeploymentOptions.TARGET),
            "No execution.target specified in your configuration file.");

    final PipelineExecutorFactory executorFactory =
            executorServiceLoader.getExecutorFactory(configuration);

    checkNotNull(
            executorFactory,
            "Cannot find compatible factory for specified execution.target (=%s)",
            configuration.get(DeploymentOptions.TARGET));

    return executorFactory.getExecutor(configuration);
}
```

executorServiceLoader.getExecutorFactory(configuration);  多个实现 看默认实现DefaultExecutorServiceLoader

```java
@Override
public PipelineExecutorFactory getExecutorFactory(final Configuration configuration) {
    checkNotNull(configuration);

    final ServiceLoader<PipelineExecutorFactory> loader =
            ServiceLoader.load(PipelineExecutorFactory.class);

    final List<PipelineExecutorFactory> compatibleFactories = new ArrayList<>();
    final Iterator<PipelineExecutorFactory> factories = loader.iterator();
    while (factories.hasNext()) {
        try {
            final PipelineExecutorFactory factory = factories.next();
            if (factory != null && factory.isCompatibleWith(configuration)) {
                compatibleFactories.add(factory);
            }
        } catch (Throwable e) {
            if (e.getCause() instanceof NoClassDefFoundError) {
                LOG.info("Could not load factory due to missing dependencies.");
            } else {
                throw e;
            }
        }
    }

    if (compatibleFactories.size() > 1) {
        final String configStr =
                configuration.toMap().entrySet().stream()
                        .map(e -> e.getKey() + "=" + e.getValue())
                        .collect(Collectors.joining("\n"));

        throw new IllegalStateException(
                "Multiple compatible client factories found for:\n" + configStr + ".");
    }

    if (compatibleFactories.isEmpty()) {
        throw new IllegalStateException("No ExecutorFactory found to execute the application.");
    }

    return compatibleFactories.get(0);
}
```

executorFactory.getExecutor(configuration); PipelineExecutorFactory主要有 KubernetesSessionClusterExecutorFactory、 YarnSessionClusterExecutorFactory、YarnJobClusterExecutorFactory、 RemoteExecutorFactory和LocalExecutorFactory实现类

PipelineExecutor主要分为三类。

·LocalExecutor:最简单的一种PipelineExecutor，仅用于本地运行 作业。

·JobClusterExecutor:通过AbstractJobClusterExecutor抽象类实现 PipelineExecutor接口，支持Per-Job模式的集群执行任务，仅被 YarnJobClusterExecutor继承和实现，即仅在Hadoop YARN集群资源管理 器中支持以Per-Job方式提交作业。

·SessionClusterExecutor:通过AbstractSessionClusterExecutor抽象类 实现PipelineExecutor接口，目前支持的SessionClusterExecutor有 RemoteExecutor、KubernetesSessionCl-usterExecutor、 YarnSessionClusterExecutor三种类型，其中RemoteExecutor主要用于 Standalone类型集群。

