所有Flink应 用程序依赖的执行环境，提供了作业运行过程中需要的环境信息。同 时，ExecutionEnvironment也提供了与外部数据源交互，创建 DataStream数据集的方法

ExecutionEnvironment分为支 持离线批计算的ExecutionEnvironment和支持流计算的 StreamExecutionEnvironment两种类型

ExecutionEnvironment 子类

·ExecutionEnvironment:批处理场景的执行环境，用于创建批处 理过程中用到的环境信息。

·RemoteEnvironment:继承自ExecutionEnvironment，用于远程将 批作业提交到集群。这种方式并不常用，且需要将构建好的应用JAR包 作为参数创建RemoteEnvironment。

·LocalEnvironment:本地批计算运行环境，即在本地JVM启动一 个MiniCluster环境，并将应用程序直接提交到MiniCluster中运行。这种 方式仅适用于本地测试场景。

·CollectionEnvironment:也是本地执行环境，主要用于从集合类 中读取数据并将其转换成DataSet数据集。仅适用于本地测试场景。

·ContextEnvironment:用于以客户端命令行的方式提交Flink作业 的场景，在客户端进程内创建ContextEnvironment，并通过 ContextEnvironment创建应用程序中的ExecutionEnvironment。

·OptimizerPlanEnvironment:用于获取DataSet API中的 OptimizerPlan信息。OptimizerPlan和StreamGraph都是Pipeline接口的实现类，OptimizerPlanEnvironment不参与具体的计算任务。

StreamExecutionEnvironment 子类

·StreamExecutionEnvironment:提供流应用程序需要的环境信息， 包括接入流数据等操作及当前应用程序需要的配置信息。

·RemoteStreamEnvironment:继承自StreamExecutionEnvironment， 主要通过远程方式将流式作业提交到集群运行，且创建过程需要以应 用程序的JAR包作为参数。

·LocalStreamEnvironment:作为流作业的本地执行环境，和 LocalEnvironment一样在本地JVM中创建MiniCluster，并将流式作业运行 在MiniCluster中。

·StreamContextEnvironment:主要用于通过客户端方式提交流式 作业，且基于ContextEnvironment构建。它会将ContextEnvironment作为 自己的成员变量。

·StreamPlanEnvironment:和OptimizerPlanEnvironment一样，仅用 于获取StreamGraph结构，本身并不参与作业的执行。

StreamExecutionEnvironment 主要成员变量

·contextEnvironmentFactory:主要用于以命令行模式提交作业， 当用户使用客户端提交作业时，就会通过contextEnvironmentFactory创 建StreamExecutionEnvironment。

·config:主要存储当前运行环境中的执行参数，例如通过执行模 式(executionMode)区分当前任务是批计算还是流计算。

·checkpointCfg:主要用于配置Checkpoint，例如是否开启了 Checkpoint，Checkpointing模式是exactly-once还是at-least-once类型等。

·transformations:DataStream和DataStream之间的转换操作都会生 成Transformation对象。例如当执行DataStream.shuffle()操作时， ExecutionEnvironment会创建对应的PartitionTransformation，并将该 Transformation添加到StreamExecutionEnvironment的transformation集合 中，最后通过transformation集合构建StreamGraph对象。

·defaultStateBackend:该成员变量代表默认的状态存储后端，默认 实现为MemoryStateBackend。

·executorServiceLoader:通过Java SPI技术加载 PipelineExecutorFactory的实现类。当通过命令行提交作业时，会通过 executorServiceLoader加载对应类型集群的PipelineExecutorFactory。

·configuration:用于执行环境中常用的K-V配置，可提供多种类 型的配置获取方法。

·userClassloader:专门为用户编写代码提供的类加载器，它不同 于Flink框架自身的类加载器。当用户提交Flink应用程序的时候， userClassloader会加载、jar文件中的所有类，并伴随应用程序的产生、 构建、提交、运行全流程。

·jobListeners:用于在应用程序通过客户端提交到集群后，向客户 端通知应用程序的执行结果，例如作业执行状态等。

初始化

```java
public static StreamExecutionEnvironment getExecutionEnvironment() {
    return getExecutionEnvironment(new Configuration());
}
```

```java
public static StreamExecutionEnvironment getExecutionEnvironment(Configuration configuration) {
    return Utils.resolveFactory(threadLocalContextEnvironmentFactory, contextEnvironmentFactory)
      //如果从threadLocalContextEnvironmentFactory或 contextEnvironmentFactory中获取 StreamExecutionEnvironmentFactory实例，会调用 StreamExecutionEnvironmentFactory.createExecutionEnvironment( )方法创建StreamExecutionEnvironment，并返回创建好的 StreamExecutionEnvironment。
      //如果上一步返回Optional.Empty()，则继续调用 StreamExecutionEnvironment.createStreamExecutionEnvironment() 方法创建执行环境。
      //createLocalEnvironment()方 法创建本地运行环境，也就是在本地JVM中启动MiniCluster，即包括 JobManager、TaskManager等主要组件的伪分布式运行环境。此时用户 编写的应用程序在本地运行环境中执行。这种情况通常出现在用户通 过IDEA启动Flink作业时，会创建LocalEnvironment运行作业
            .map(factory -> factory.createExecutionEnvironment(configuration))
            .orElseGet(() -> StreamExecutionEnvironment.createLocalEnvironment(configuration));
}
```

调用Utils.resolveFactory()方法分别从ThreadLocal变量 threadLocalContextEnvironmentFactory和 ContextEnvironmentFactory中获取StreamExecutionEnvironment工厂创建类。在Utils.resolveFactory()方法定义中，分别从 threadLocalContextEnvironmentFactory和静态 contextEnvironmentFactory中获取localFactory和staticFactory， 如果都没有获取到，则返回Optional.Empty()方法;如果能获取到其 中任何一个，则将Factory放置在Optional中。下一步利用获取到的工 厂类实现类创建StreamExecutionEnvironment。在Scala Shell交互模 式下，通过ContextEnvironmentFactory提供的工厂方法创建 ContextEnvironment。

Utils.resolveFactory()

```java
public static <T> Optional<T> resolveFactory(
        ThreadLocal<T> threadLocalFactory, @Nullable T staticFactory) {
    final T localFactory = threadLocalFactory.get();
    final T factory = localFactory == null ? staticFactory : localFactory;

    return Optional.ofNullable(factory);
}
```

StreamExecutionEnvironmentFactory

```java
@PublicEvolving
@FunctionalInterface
public interface StreamExecutionEnvironmentFactory {
//两个实现类 StreamPlanEnvironment StreamContextEnvironment
    /**
     * Creates a StreamExecutionEnvironment from this factory.
     *
     * @return A StreamExecutionEnvironment.
     */
    StreamExecutionEnvironment createExecutionEnvironment(Configuration configuration);
}
```



```java
public static void setAsContext(
        final PipelineExecutorServiceLoader executorServiceLoader,
        final Configuration clusterConfiguration,
        final ClassLoader userCodeClassLoader,
        final boolean enforceSingleJobExecution,
        final boolean suppressSysout) {
    final StreamExecutionEnvironmentFactory factory =
            envInitConfig -> {
                final boolean programConfigEnabled =
                        clusterConfiguration.get(DeploymentOptions.PROGRAM_CONFIG_ENABLED);
                final List<String> programConfigWildcards =
                        clusterConfiguration.get(DeploymentOptions.PROGRAM_CONFIG_WILDCARDS);
                final Configuration mergedEnvConfig = new Configuration();
                mergedEnvConfig.addAll(clusterConfiguration);
                mergedEnvConfig.addAll(envInitConfig);
      //StreamContextEnvironment 继承 StreamExecutionEnvironment
                return new StreamContextEnvironment(
                        executorServiceLoader,
                        clusterConfiguration,
                        mergedEnvConfig,
                        userCodeClassLoader,
                        enforceSingleJobExecution,
                        suppressSysout,
                        programConfigEnabled,
                        programConfigWildcards);
            };
    initializeContextEnvironment(factory);
}
```

执行 exec()

```java
public JobExecutionResult execute() throws Exception {
    return execute((String) null);
}
```

```java
public JobExecutionResult execute(String jobName) throws Exception {
    final List<Transformation<?>> originalTransformations = new ArrayList<>(transformations);
    StreamGraph streamGraph = getStreamGraph();//获取StreamGraph
    if (jobName != null) {
        streamGraph.setJobName(jobName);
    }

    try {
        return execute(streamGraph);//执行
    } catch (Throwable t) {
        Optional<ClusterDatasetCorruptedException> clusterDatasetCorruptedException =
                ExceptionUtils.findThrowable(t, ClusterDatasetCorruptedException.class);
        if (!clusterDatasetCorruptedException.isPresent()) {
            throw t;
        }

        // Retry without cache if it is caused by corrupted cluster dataset.
        invalidateCacheTransformations(originalTransformations);
        streamGraph = getStreamGraph(originalTransformations);
        return execute(streamGraph);//catch 中执行
    }
}
```

```java
@Internal
public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
    final JobClient jobClient = executeAsync(streamGraph);//异步执行

    try {
        final JobExecutionResult jobExecutionResult;
//判断是ATTACHED还是Detached
        if (configuration.getBoolean(DeploymentOptions.ATTACHED)) {
            jobExecutionResult = jobClient.getJobExecutionResult().get();
        } else {
            jobExecutionResult = new DetachedJobExecutionResult(jobClient.getJobID());
        }

        jobListeners.forEach(
                jobListener -> jobListener.onJobExecuted(jobExecutionResult, null));

        return jobExecutionResult;
    } catch (Throwable t) {
        // get() on the JobExecutionResult Future will throw an ExecutionException. This
        // behaviour was largely not there in Flink versions before the PipelineExecutor
        // refactoring so we should strip that exception.
        Throwable strippedException = ExceptionUtils.stripExecutionException(t);

        jobListeners.forEach(
                jobListener -> {
                    jobListener.onJobExecuted(null, strippedException);
                });
        ExceptionUtils.rethrowException(strippedException);

        // never reached, only make javac happy
        return null;
    }
}
```

```java
@Internal
public JobClient executeAsync(StreamGraph streamGraph) throws Exception {
    checkNotNull(streamGraph, "StreamGraph cannot be null.");
    final PipelineExecutor executor = getPipelineExecutor();
  //StreamGraph转换为JobGraph的过程主要借助客户端的PipelineExecutor实现

    CompletableFuture<JobClient> jobClientFuture =
            executor.execute(streamGraph, configuration, userClassloader);

    try {
        JobClient jobClient = jobClientFuture.get();
      //将jobClient添加到jobListener集合中，用于监听任务执行状态
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

executor.execute(streamGraph, configuration, userClassloader); PipelineExecutor主要分为三类。

·LocalExecutor:最简单的一种PipelineExecutor，仅用于本地运行 作业。

·JobClusterExecutor:通过AbstractJobClusterExecutor抽象类实现 PipelineExecutor接口，支持Per-Job模式的集群执行任务，仅被 YarnJobClusterExecutor继承和实现，即仅在Hadoop YARN集群资源管理 器中支持以Per-Job方式提交作业。

·SessionClusterExecutor:通过AbstractSessionClusterExecutor抽象类 实现PipelineExecutor接口，目前支持的SessionClusterExecutor有 RemoteExecutor、KubernetesSessionCl-usterExecutor、 YarnSessionClusterExecutor三种类型，其中RemoteExecutor主要用于 Standalone类型集群。