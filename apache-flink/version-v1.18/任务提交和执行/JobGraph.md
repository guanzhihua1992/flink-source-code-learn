1)用户通过API编写应用程序，将可执行JAR包通过客户端提交到 集群中运行，此时在客户端将DataStream转换操作集合保存至 StreamExecutionEnvironment的Transformation集合。
2)通过StreamGraphGenerator对象将Transformation集合转换为 StreamGraph。
3)在PipelineExector中将StreamGraph对象转换成JobGraph数据 结构。JobGraph结构是所有类型客户端和集群之间的任务提交协议， 不管是哪种类型的Flink应用程序，最终都会转换成JobGraph提交到集 群运行时中运行。
4)集群运行时接收到JobGraph之后，会通过JobGraph创建和启动 相应的JobManager服务，并在JobManager服务中将JobGraph转换为 ExecutionGraph。
5)JobManager会根据ExecutionGraph中的节点进行调度，实际上 就是将具体的Task部署到TaskManager中进行调度和执行。

JobGraph是所有类型作业与集 群运行时之间的通信协议，相比于StreamGraph结构，JobGraph主要增 加了系统执行参数及依赖等信息，如作业依赖的JAR包等

JobGraph数据结构在本质上是将 节点和中间结果集相连得到有向无环图。JobGraph是客户端和运行时 之间进行作业提交使用的统一数据结构，不管是流式(StreamGraph) 还是批量(OptimizerPlan)，最终都会转换成集群接受的JobGraph数 据结构。之所以会进行这么多层图转换，也是为了更好地兼容离线和 流式作业，让不同类型的作业都可以运行在同一套集群运行时中。

JobGraph数据结构主要包含如下成员变量。

·jobID:当前Job对应的ID。

·taskVertices:存储了当前Job包含的所有节点，每个节点通过 JobVertex结构表示。

·jobConfiguration:存储了当前Job用到的配置信息。

·scheduleMode:当前Job启用的Task调度模式，流式作业中默认的 调度模式是EAGER类型。

·snapshotSettings:存储当前Job使用的Checkpoint配置信息。

·savepointRestoreSettings:存储当前Job用来恢复任务的Savepoint配 置信息。

·userJars:存储当前作业依赖JAR包的地址。在将作业提交到集群 中的过程中，这些JAR包会通过网络上传到运行时的BlobServer中，在应 用程序中的Task执行时会将相关JAR包下载到TaskManager本地路径，并 加载到Task线程所在的UserClassLoader中。

·userArtifacts:当前作业需要使用的自定义文件，并通过 DistributedCacheEntry表示。

·userJarBlobKeys:将JAR包上传到BlobServer之后，返回的BlobKey 地址信息会存储在该集合中。

·classpaths:当前作业对应的classpath信息。

JopGraph数据结构是Flink客户端与集群交互的统一数据结构，不 管是批数据处理的DataSet API、流数据处理的DataStream API，还是 Table/SQL API，最终都会将作业转换成JobGraph提交到集群中运行。

PipelineExecutor中支持远程任务执行的Executor主要有两种类 型，分别为AbstractJobClusterExecutor和 AbstractSessionClusterExecutor。不管是哪种类型的 PipelineExecutor实现，最终都会在各自的execute()方法中调用 PipelineExecutorUtils.getJobGraph()方法将pipeline转换为JobGraph结构。

```java
@Override
public CompletableFuture<JobClient> execute(
        @Nonnull final Pipeline pipeline,
        @Nonnull final Configuration configuration,
        @Nonnull final ClassLoader userCodeClassloader)
        throws Exception {
  //获取JobGraph
    final JobGraph jobGraph =
            PipelineExecutorUtils.getJobGraph(pipeline, configuration, userCodeClassloader);
//通过clusterClientFactory获取ClusterDescriptor。 ClusterDescriptor是对不同类型的集群的描述，主要用于创建 Session集群和获取与集群通信的ClusterClient。
    try (final ClusterDescriptor<ClusterID> clusterDescriptor =
            clusterClientFactory.createClusterDescriptor(configuration)) {
        final ExecutionConfigAccessor configAccessor =
                ExecutionConfigAccessor.fromConfiguration(configuration);

        final ClusterSpecification clusterSpecification =
                clusterClientFactory.getClusterSpecification(configuration);
//部署deployJobCluster
        final ClusterClientProvider<ClusterID> clusterClientProvider =
                clusterDescriptor.deployJobCluster(
                        clusterSpecification, jobGraph, configAccessor.getDetachedMode());
        LOG.info("Job has been submitted with JobID " + jobGraph.getJobID());

        return CompletableFuture.completedFuture(
                new ClusterClientJobClientAdapter<>(
                        clusterClientProvider, jobGraph.getJobID(), userCodeClassloader));
    }
}
```

clusterDescriptor.deployJobCluster() 按资源类型有多个实现 yarn standalone kubernetes看下 YarnClusterDescriptor实现

```java
@Override
public ClusterClientProvider<ApplicationId> deployJobCluster(
        ClusterSpecification clusterSpecification, JobGraph jobGraph, boolean detached)
        throws ClusterDeploymentException {

    LOG.warn(
            "Job Clusters are deprecated since Flink 1.15. Please use an Application Cluster/Application Mode instead.");
    try {
        return deployInternal(
                clusterSpecification,
                "Flink per-job cluster",
                getYarnJobClusterEntrypoint(),
                jobGraph,
                detached);
    } catch (Exception e) {
        throw new ClusterDeploymentException("Could not deploy Yarn job cluster.", e);
    }
}
```

deployInternal()

```java
/**
 * This method will block until the ApplicationMaster/JobManager have been deployed on YARN.
 *
 * @param clusterSpecification Initial cluster specification for the Flink cluster to be
 *     deployed
 * @param applicationName name of the Yarn application to start
 * @param yarnClusterEntrypoint Class name of the Yarn cluster entry point.
 * @param jobGraph A job graph which is deployed with the Flink cluster, {@code null} if none
 * @param detached True if the cluster should be started in detached mode
 */
private ClusterClientProvider<ApplicationId> deployInternal(
        ClusterSpecification clusterSpecification,
        String applicationName,
        String yarnClusterEntrypoint,
        @Nullable JobGraph jobGraph,
        boolean detached)
        throws Exception {
//鉴权
    final UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    if (HadoopUtils.isKerberosSecurityEnabled(currentUser)) {
        boolean useTicketCache =
                flinkConfiguration.getBoolean(SecurityOptions.KERBEROS_LOGIN_USETICKETCACHE);

        if (!HadoopUtils.areKerberosCredentialsValid(currentUser, useTicketCache)) {
            throw new RuntimeException(
                    "Hadoop security with Kerberos is enabled but the login user "
                            + "does not have Kerberos credentials or delegation tokens!");
        }

        final boolean fetchToken =
                flinkConfiguration.getBoolean(SecurityOptions.KERBEROS_FETCH_DELEGATION_TOKEN);
        final boolean yarnAccessFSEnabled =
                !CollectionUtil.isNullOrEmpty(
                        flinkConfiguration.get(
                                SecurityOptions.KERBEROS_HADOOP_FILESYSTEMS_TO_ACCESS));
        if (!fetchToken && yarnAccessFSEnabled) {
            throw new IllegalConfigurationException(
                    String.format(
                            "When %s is disabled, %s must be disabled as well.",
                            SecurityOptions.KERBEROS_FETCH_DELEGATION_TOKEN.key(),
                            SecurityOptions.KERBEROS_HADOOP_FILESYSTEMS_TO_ACCESS.key()));
        }
    }
//检查了jar路径和core 数量
    isReadyForDeployment(clusterSpecification);

    // ------------------ Check if the specified queue exists --------------------
//检查specified queue的存在
    checkYarnQueues(yarnClient);

    // ------------------ Check if the YARN ClusterClient has the requested resources
    // --------------

    // Create application via yarnClient
  //yarnClient 创建YarnClientApplication 并获取appResponse 获取资源剩余freeClusterMem 内存
    final YarnClientApplication yarnApplication = yarnClient.createApplication();
    final GetNewApplicationResponse appResponse = yarnApplication.getNewApplicationResponse();

    Resource maxRes = appResponse.getMaximumResourceCapability();

    final ClusterResourceDescription freeClusterMem;
    try {
        freeClusterMem = getCurrentFreeClusterResources(yarnClient);
    } catch (YarnException | IOException e) {
        failSessionDuringDeployment(yarnClient, yarnApplication);
        throw new YarnDeploymentException(
                "Could not retrieve information about free cluster resources.", e);
    }

    final int yarnMinAllocationMB =
            yarnConfiguration.getInt(
                    YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
                    YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    if (yarnMinAllocationMB <= 0) {
        throw new YarnDeploymentException(
                "The minimum allocation memory "
                        + "("
                        + yarnMinAllocationMB
                        + " MB) configured via '"
                        + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB
                        + "' should be greater than 0.");
    }
//检验 内存是否足够 启动 jobManager taskManager 所需
    final ClusterSpecification validClusterSpecification;
    try {
        validClusterSpecification =
                validateClusterResources(
                        clusterSpecification, yarnMinAllocationMB, maxRes, freeClusterMem);
    } catch (YarnDeploymentException yde) {
        failSessionDuringDeployment(yarnClient, yarnApplication);
        throw yde;
    }

    LOG.info("Cluster specification: {}", validClusterSpecification);

    final ClusterEntrypoint.ExecutionMode executionMode =
            detached
                    ? ClusterEntrypoint.ExecutionMode.DETACHED
                    : ClusterEntrypoint.ExecutionMode.NORMAL;

    flinkConfiguration.setString(
            ClusterEntrypoint.INTERNAL_CLUSTER_EXECUTION_MODE, executionMode.toString());

    ApplicationReport report =
      //启动 startAppMaster
            startAppMaster(
                    flinkConfiguration,
                    applicationName,
                    yarnClusterEntrypoint,
                    jobGraph,
                    yarnClient,
                    yarnApplication,
                    validClusterSpecification);

    // print the application id for user to cancel themselves.
    if (detached) {
        final ApplicationId yarnApplicationId = report.getApplicationId();
        logDetachedClusterInformation(yarnApplicationId, LOG);
    }

    setClusterEntrypointInfoToConfig(report);

    return () -> {
        try {
            return new RestClusterClient<>(flinkConfiguration, report.getApplicationId());
        } catch (Exception e) {
            throw new RuntimeException("Error while creating RestClusterClient.", e);
        }
    };
}
```

startAppMaster()启动 几百行代码 不贴了 可以细看 

回头看下AbstractSessionClusterExecutor的execute()

```java
@Override
public CompletableFuture<JobClient> execute(
        @Nonnull final Pipeline pipeline,
        @Nonnull final Configuration configuration,
        @Nonnull final ClassLoader userCodeClassloader)
        throws Exception {
  //调用getJobGraph()方法获取JobGraph，实际上就是调用 FlinkPipelineTranslationUtil.getJobGraph()方法获取JobGraph。
    final JobGraph jobGraph =
            PipelineExecutorUtils.getJobGraph(pipeline, configuration, userCodeClassloader);

    try (final ClusterDescriptor<ClusterID> clusterDescriptor =
            clusterClientFactory.createClusterDescriptor(configuration)) {
        final ClusterID clusterID = clusterClientFactory.getClusterId(configuration);
        checkState(clusterID != null);

        final ClusterClientProvider<ClusterID> clusterClientProvider =
                clusterDescriptor.retrieve(clusterID);
        ClusterClient<ClusterID> clusterClient = clusterClientProvider.getClusterClient();
        return clusterClient
                .submitJob(jobGraph)
                .thenApplyAsync(
                        FunctionUtils.uncheckedFunction(
                                jobId -> {
                                    ClientUtils.waitUntilJobInitializationFinished(
                                            () -> clusterClient.getJobStatus(jobId).get(),
                                            () -> clusterClient.requestJobResult(jobId).get(),
                                            userCodeClassloader);
                                    return jobId;
                                }))
                .thenApplyAsync(
                        jobID ->
                                (JobClient)
                                        new ClusterClientJobClientAdapter<>(
                                                clusterClientProvider,
                                                jobID,
                                                userCodeClassloader))
                .whenCompleteAsync((ignored1, ignored2) -> clusterClient.close());
    }
}
```

clusterClient.submitJob(jobGraph) RestClusterClient和MiniClusterClient两个

MiniClusterClient

```java
@Override
public CompletableFuture<JobID> submitJob(@Nonnull JobGraph jobGraph) {
    return miniCluster.submitJob(jobGraph).thenApply(JobSubmissionResult::getJobID);
}
```

```java
public CompletableFuture<JobSubmissionResult> submitJob(JobGraph jobGraph) {
    // When MiniCluster uses the local RPC, the provided JobGraph is passed directly to the
    // Dispatcher. This means that any mutations to the JG can affect the Dispatcher behaviour,
    // so we rather clone it to guard against this.
    final JobGraph clonedJobGraph = InstantiationUtil.cloneUnchecked(jobGraph);
    checkRestoreModeForChangelogStateBackend(clonedJobGraph);
    final CompletableFuture<DispatcherGateway> dispatcherGatewayFuture =
            getDispatcherGatewayFuture();
    final CompletableFuture<InetSocketAddress> blobServerAddressFuture =
            createBlobServerAddress(dispatcherGatewayFuture);
    final CompletableFuture<Void> jarUploadFuture =
            uploadAndSetJobFiles(blobServerAddressFuture, clonedJobGraph);
    final CompletableFuture<Acknowledge> acknowledgeCompletableFuture =
            jarUploadFuture
                    .thenCombine(
                            dispatcherGatewayFuture,
                            (Void ack, DispatcherGateway dispatcherGateway) ->
                                    dispatcherGateway.submitJob(clonedJobGraph, rpcTimeout))
                    .thenCompose(Function.identity());
    return acknowledgeCompletableFuture.thenApply(
            (Acknowledge ignored) -> new JobSubmissionResult(clonedJobGraph.getJobID()));
}
```

dispatcherGateway.submitJob(clonedJobGraph, rpcTimeout)

final JobGraph jobGraph =
            PipelineExecutorUtils.getJobGraph(pipeline, configuration, userCodeClassloader);

```java
public static JobGraph getJobGraph(
        @Nonnull final Pipeline pipeline,
        @Nonnull final Configuration configuration,
        @Nonnull ClassLoader userClassloader)
        throws MalformedURLException {
    checkNotNull(pipeline);
    checkNotNull(configuration);

    final ExecutionConfigAccessor executionConfigAccessor =
            ExecutionConfigAccessor.fromConfiguration(configuration);
    final JobGraph jobGraph =
            FlinkPipelineTranslationUtil.getJobGraph(
                    userClassloader,
                    pipeline,
                    configuration,
                    executionConfigAccessor.getParallelism());//核心

    configuration
            .getOptional(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID)
            .ifPresent(strJobID -> jobGraph.setJobID(JobID.fromHexString(strJobID)));

    if (configuration.getBoolean(DeploymentOptions.ATTACHED)
            && configuration.getBoolean(DeploymentOptions.SHUTDOWN_IF_ATTACHED)) {
        jobGraph.setInitialClientHeartbeatTimeout(
                configuration.getLong(ClientOptions.CLIENT_HEARTBEAT_TIMEOUT));
    }

    jobGraph.addJars(executionConfigAccessor.getJars());//jars
    jobGraph.setClasspaths(executionConfigAccessor.getClasspaths());//classpaths
    jobGraph.setSavepointRestoreSettings(executionConfigAccessor.getSavepointRestoreSettings());//savepointrestore

    return jobGraph;
}
```

FlinkPipelineTranslationUtil.getJobGraph()

```java
/** Transmogrifies the given {@link Pipeline} to a {@link JobGraph}. */
public static JobGraph getJobGraph(
        ClassLoader userClassloader,
        Pipeline pipeline,
        Configuration optimizerConfiguration,
        int defaultParallelism) {

    FlinkPipelineTranslator pipelineTranslator =
            getPipelineTranslator(userClassloader, pipeline);

    return pipelineTranslator.translateToJobGraph(
            pipeline, optimizerConfiguration, defaultParallelism);
}
```

getPipelineTranslator

```java
private static FlinkPipelineTranslator getPipelineTranslator(
        ClassLoader userClassloader, Pipeline pipeline) {
    PlanTranslator planTranslator = new PlanTranslator();

    if (planTranslator.canTranslate(pipeline)) {
        return planTranslator;
    }

    StreamGraphTranslator streamGraphTranslator = new StreamGraphTranslator(userClassloader);

    if (streamGraphTranslator.canTranslate(pipeline)) {
        return streamGraphTranslator;
    }

    throw new RuntimeException(
            "Translator "
                    + streamGraphTranslator
                    + " cannot translate "
                    + "the given pipeline "
                    + pipeline
                    + ".");
}
```

pipelineTranslator.translateToJobGraph() FlinkPipelineTranslator 有两个实现类 

PlanTranslator StreamGraphTranslator 分别对应批流

```java
/** {@link FlinkPipelineTranslator} for DataSet API {@link Plan Plans}. */
public class PlanTranslator implements FlinkPipelineTranslator {}
```

```java
/**
 * {@link FlinkPipelineTranslator} for DataStream API {@link StreamGraph StreamGraphs}.
 *
 * <p>Note: this is used through reflection in {@link
 * org.apache.flink.client.FlinkPipelineTranslationUtil}.
 */
@SuppressWarnings("unused")
public class StreamGraphTranslator implements FlinkPipelineTranslator {}
```

StreamGraphTranslator.translateToJobGraph()

```java
@Override
public JobGraph translateToJobGraph(
        Pipeline pipeline, Configuration optimizerConfiguration, int defaultParallelism) {
    checkArgument(
            pipeline instanceof StreamGraph, "Given pipeline is not a DataStream StreamGraph.");

    StreamGraph streamGraph = (StreamGraph) pipeline;
    return streamGraph.getJobGraph(userClassloader, null);
}
```



```java
/** Gets the assembled {@link JobGraph} with a specified {@link JobID}. */
public JobGraph getJobGraph(ClassLoader userClassLoader, @Nullable JobID jobID) {
    return StreamingJobGraphGenerator.createJobGraph(userClassLoader, this, jobID);
}
```

StreamingJobGraphGenerator.createJobGraph()

```java
public static JobGraph createJobGraph(
        ClassLoader userClassLoader, StreamGraph streamGraph, @Nullable JobID jobID) {
    // TODO Currently, we construct a new thread pool for the compilation of each job. In the
    // future, we may refactor the job submission framework and make it reusable across jobs.
  //现在是每个job都来一个线程池 后面是想把这个池公用
    final ExecutorService serializationExecutor =
            Executors.newFixedThreadPool(
                    Math.max(
                            1,
                            Math.min(
                                    Hardware.getNumberCPUCores(),
                                    streamGraph.getExecutionConfig().getParallelism())),
                    new ExecutorThreadFactory("flink-operator-serialization-io"));
    try {//new StreamingJobGraphGenerator()中把streamGraph传入并new了一个JobGraph
        return new StreamingJobGraphGenerator(
                        userClassLoader, streamGraph, jobID, serializationExecutor)
                .createJobGraph();
    } finally {
        serializationExecutor.shutdown();
    }
}
```

StreamingJobGraphGenerator.createJobGraph()

```java
private JobGraph createJobGraph() {
  //调用preValidate()方法对StreamGraph进行预检查，例如检查 在开启Checkpoint的情况下，StreamGraph每个节点的Operator是否实 现InputSelectable接口。
    preValidate();
    jobGraph.setJobType(streamGraph.getJobType());
    jobGraph.setDynamic(streamGraph.isDynamic());

    jobGraph.enableApproximateLocalRecovery(
            streamGraph.getCheckpointConfig().isApproximateLocalRecoveryEnabled());

    // Generate deterministic hashes for the nodes in order to identify them across
    // submission iff they didn't change.
  //对StreamGraph的StreamNode进行哈希化处理，用于生成 JobVertexID。在StreamGraph中StreamNodeID为数字表示，而在 JobGraph中JobVertexID由哈希码生成，通过哈希码区分JobGraph中的 节点。
    Map<Integer, byte[]> hashes =
            defaultStreamGraphHasher.traverseStreamGraphAndGenerateHashes(streamGraph);

    // Generate legacy version hashes for backwards compatibility
  //生成历史legacyHashes代码，这一步主要是为了与之前的版本兼容。
    List<Map<Integer, byte[]>> legacyHashes = new ArrayList<>(legacyStreamGraphHashers.size());
    for (StreamGraphHasher hasher : legacyStreamGraphHashers) {
        legacyHashes.add(hasher.traverseStreamGraphAndGenerateHashes(streamGraph));
    }
//转换
    setChaining(hashes, legacyHashes);
//判读是都动态 调整并行度
    if (jobGraph.isDynamic()) {
        setVertexParallelismsForDynamicGraphIfNecessary();
    }

    // Note that we set all the non-chainable outputs configuration here because the
    // "setVertexParallelismsForDynamicGraphIfNecessary" may affect the parallelism of job
    // vertices and partition-reuse
    final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs =
            new HashMap<>();
    setAllOperatorNonChainedOutputsConfigs(opIntermediateOutputs);
    setAllVertexNonChainedOutputsConfigs(opIntermediateOutputs);

    setPhysicalEdges();

    markSupportingConcurrentExecutionAttempts();

    validateHybridShuffleExecuteInBatchMode();
//设定当前JobGraph 中的SlotSharing及CoLocation策略。
    setSlotSharingAndCoLocation();
//设定JobGraph中的管理 内存比例。
    setManagedMemoryFraction(
            Collections.unmodifiableMap(jobVertices),
            Collections.unmodifiableMap(vertexConfigs),
            Collections.unmodifiableMap(chainedConfigs),
            id -> streamGraph.getStreamNode(id).getManagedMemoryOperatorScopeUseCaseWeights(),
            id -> streamGraph.getStreamNode(id).getManagedMemorySlotScopeUseCases());
//设置JobGraph中的 checkpoint配置信息。
    configureCheckpointing();
//设定当前JobGraph中的 SavepointRestoreSettings参数。
    jobGraph.setSavepointRestoreSettings(streamGraph.getSavepointRestoreSettings());
//用户在 Job中用到的自定义文件等。
    final Map<String, DistributedCache.DistributedCacheEntry> distributedCacheEntries =
            JobGraphUtils.prepareUserArtifactEntries(
                    streamGraph.getUserArtifacts().stream()
                            .collect(Collectors.toMap(e -> e.f0, e -> e.f1)),
                    jobGraph.getJobID());

    for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry :
            distributedCacheEntries.entrySet()) {
        jobGraph.addUserArtifact(entry.getKey(), entry.getValue());
    }

    // set the ExecutionConfig last when it has been finalized
  //StreamGraph中的ExecutionConfig设定到JobGraph中。
    try {
        jobGraph.setExecutionConfig(streamGraph.getExecutionConfig());
    } catch (IOException e) {
        throw new IllegalConfigurationException(
                "Could not serialize the ExecutionConfig."
                        + "This indicates that non-serializable types (like custom serializers) were registered");
    }

    jobGraph.setChangelogStateBackendEnabled(streamGraph.isChangelogStateBackendEnabled());

    addVertexIndexPrefixInVertexName();

    setVertexDescription();

    // Wait for the serialization of operator coordinators and stream config.
    try {
        FutureUtils.combineAll(
                        vertexConfigs.values().stream()
                                .map(
                                        config ->
                                                config.triggerSerializationAndReturnFuture(
                                                        serializationExecutor))
                                .collect(Collectors.toList()))
                .get();

        waitForSerializationFuturesAndUpdateJobVertices();
    } catch (Exception e) {
        throw new FlinkRuntimeException("Error in serialization.", e);
    }

    if (!streamGraph.getJobStatusHooks().isEmpty()) {
        jobGraph.setJobStatusHooks(streamGraph.getJobStatusHooks());
    }

    return jobGraph;
}
```

setChaining(hashes, legacyHashes);根据每个节点生成的哈希码从源节 点开始递归创建JobVertex节点，此处会将多个符合条件的StreamNode 节点链化在一个JobVertex节点中。执行过程中会根据JobVertex创建 OperatorChain，以减少数据在TaskManager之间网络传输的性能消耗。

```java
/**
 * Sets up task chains from the source {@link StreamNode} instances.
 *
 * <p>This will recursively create all {@link JobVertex} instances.
 */
private void setChaining(Map<Integer, byte[]> hashes, List<Map<Integer, byte[]>> legacyHashes) {
    // we separate out the sources that run as inputs to another operator (chained inputs)
    // from the sources that needs to run as the main (head) operator.
    final Map<Integer, OperatorChainInfo> chainEntryPoints =
            buildChainedInputsAndGetHeadInputs(hashes, legacyHashes);
    final Collection<OperatorChainInfo> initialEntryPoints =
            chainEntryPoints.entrySet().stream()
                    .sorted(Comparator.comparing(Map.Entry::getKey))
                    .map(Map.Entry::getValue)
                    .collect(Collectors.toList());

    // iterate over a copy of the values, because this map gets concurrently modified
    for (OperatorChainInfo info : initialEntryPoints) {
        createChain(
                info.getStartNodeId(),
                1, // operators start at position 1 because 0 is for chained source inputs
                info,
                chainEntryPoints);
    }
}
```

buildChainedInputsAndGetHeadInputs(hashes, legacyHashes);

```java
private Map<Integer, OperatorChainInfo> buildChainedInputsAndGetHeadInputs(
        final Map<Integer, byte[]> hashes, final List<Map<Integer, byte[]>> legacyHashes) {

    final Map<Integer, ChainedSourceInfo> chainedSources = new HashMap<>();
    final Map<Integer, OperatorChainInfo> chainEntryPoints = new HashMap<>();

    for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
        final StreamNode sourceNode = streamGraph.getStreamNode(sourceNodeId);

        if (sourceNode.getOperatorFactory() instanceof SourceOperatorFactory
                && sourceNode.getOutEdges().size() == 1) {
            // as long as only NAry ops support this chaining, we need to skip the other parts
            final StreamEdge sourceOutEdge = sourceNode.getOutEdges().get(0);
            final StreamNode target = streamGraph.getStreamNode(sourceOutEdge.getTargetId());
            final ChainingStrategy targetChainingStrategy =
                    target.getOperatorFactory().getChainingStrategy();

            if (targetChainingStrategy == ChainingStrategy.HEAD_WITH_SOURCES
                    && isChainableInput(sourceOutEdge, streamGraph)) {
                final OperatorID opId = new OperatorID(hashes.get(sourceNodeId));
                final StreamConfig.SourceInputConfig inputConfig =
                        new StreamConfig.SourceInputConfig(sourceOutEdge);
                final StreamConfig operatorConfig = new StreamConfig(new Configuration());
                setOperatorConfig(sourceNodeId, operatorConfig, Collections.emptyMap());
                setOperatorChainedOutputsConfig(operatorConfig, Collections.emptyList());
                // we cache the non-chainable outputs here, and set the non-chained config later
                opNonChainableOutputsCache.put(sourceNodeId, Collections.emptyList());

                operatorConfig.setChainIndex(0); // sources are always first
                operatorConfig.setOperatorID(opId);
                operatorConfig.setOperatorName(sourceNode.getOperatorName());
                chainedSources.put(
                        sourceNodeId, new ChainedSourceInfo(operatorConfig, inputConfig));

                final SourceOperatorFactory<?> sourceOpFact =
                        (SourceOperatorFactory<?>) sourceNode.getOperatorFactory();
                final OperatorCoordinator.Provider coord =
                        sourceOpFact.getCoordinatorProvider(sourceNode.getOperatorName(), opId);

                final OperatorChainInfo chainInfo =
                        chainEntryPoints.computeIfAbsent(
                                sourceOutEdge.getTargetId(),
                                (k) ->
                                        new OperatorChainInfo(
                                                sourceOutEdge.getTargetId(),
                                                hashes,
                                                legacyHashes,
                                                chainedSources,
                                                streamGraph));
                chainInfo.addCoordinatorProvider(coord);
                chainInfo.recordChainedNode(sourceNodeId);
                continue;
            }
        }

        chainEntryPoints.put(
                sourceNodeId,
                new OperatorChainInfo(
                        sourceNodeId, hashes, legacyHashes, chainedSources, streamGraph));
    }

    return chainEntryPoints;
}
```

PipelineExecutor.execute() 方法执行JobGraph，这个过程会将 JobGraph提交到集群中运行。