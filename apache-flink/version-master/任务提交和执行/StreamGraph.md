1)用户通过API编写应用程序，将可执行JAR包通过客户端提交到 集群中运行，此时在客户端将DataStream转换操作集合保存至 StreamExecutionEnvironment的Transformation集合。
2)通过StreamGraphGenerator对象将Transformation集合转换为 StreamGraph。
3)在PipelineExector中将StreamGraph对象转换成JobGraph数据 结构。JobGraph结构是所有类型客户端和集群之间的任务提交协议， 不管是哪种类型的Flink应用程序，最终都会转换成JobGraph提交到集 群运行时中运行。
4)集群运行时接收到JobGraph之后，会通过JobGraph创建和启动 相应的JobManager服务，并在JobManager服务中将JobGraph转换为 ExecutionGraph。
5)JobManager会根据ExecutionGraph中的节点进行调度，实际上 就是将具体的Task部署到TaskManager中进行调度和执行。

StreamGraph结构描述了作业的逻辑拓扑结构，并以有向无环图 的形式描述作业中算子之间的上下游连接关系。

StreamGraph结构是由StreamGraphGenerator通过Transformation 集合转换而来,StreamGraph实现了 Pipeline的接口，且通过有向无环图的结构描述了DataStream作业的 拓扑关系。StreamGraph结构包含StreamEdge和StreamNode等结构，其 中StreamEdge代表StreamGraph的边，StreamNode代表StreamGraph的 节点。此外，StreamGraph结构还包含任务调度模式ScheduleMode及 TimeCharacteristic时间概念类型等与作业相关的参数。

StreamGraph构建和提交主要步骤

1)调用StreamExecutionEnvironment.execute()方法，执行整个 作业。在基于客户端命令行方式提交作业时，会通过PackagedProgram 执行应用程序中的main()方法，然后运行应用程序中的 StreamExecutionEnvironment.execute()方法。

2)调用StreamExecutionEnvironment.getStreamGraph()方法获 取StreamGraph。

3)调用 StreamExecutionEnvironment.getStreamGraphGenerator()方法获取 StreamGraphGenerator对象。

4)调用StreamGraphGenerator.generate()方法生成StreamGraph 对象。

5)返回StreamExecutionEnvironment创建的StreamGraph对象。

6)继续调用 StreamExecutionEnvironment.executeAsync(streamGraph)方法，执 行创建好的StreamGraph对象。

7)在StreamExecutionEnvironment中调用 PipelineExecutorServiceLoader加载PipelineExecutorFactory实 例，PipelineExecutorServiceLoader会根据执行环境的服务配置创建 PipelineExecutorFactory。

8)PipelineExecutorFactory加载完成后，调用 PipelineExecutorFactory.getExecutor()方法创建 PipelineExecutor。

9)调用PipelineExecutor.execute()方法执行创建好的 StreamGraph，此时方法会向StreamExecutionEnvironment返回异步客 户端jobClientFuture。

10)StreamExecutionEnvironment调用jobClientFuture.get()方 法得到同步JobClient对象。

11)JobClient将Job的执行结果返回到 StreamExecutionEnvironment。

12)通过JobClient获取作业提交后的执行情况，并将应用程序返 回的结果返回给用户。

起点 StreamExecutionEnvironment.execute()

```java
public JobExecutionResult execute(String jobName) throws Exception {
    final List<Transformation<?>> originalTransformations = new ArrayList<>(transformations);
    StreamGraph streamGraph = getStreamGraph();
    ...
        return execute(streamGraph);
    }
}
```

```java
@Internal
public StreamGraph getStreamGraph() {
    return getStreamGraph(true);
}
```

```java
@Internal
public StreamGraph getStreamGraph(boolean clearTransformations) {
    final StreamGraph streamGraph = getStreamGraph(transformations);
    if (clearTransformations) {
        transformations.clear();
    }
    return streamGraph;
}
```

```java
private StreamGraph getStreamGraph(List<Transformation<?>> transformations) {
    synchronizeClusterDatasetStatus();
    return getStreamGraphGenerator(transformations).generate();
}
```

getStreamGraphGenerator

```java
private StreamGraphGenerator getStreamGraphGenerator(List<Transformation<?>> transformations) {
    if (transformations.size() <= 0) {
        throw new IllegalStateException(
                "No operators defined in streaming topology. Cannot execute.");
    }

    // We copy the transformation so that newly added transformations cannot intervene with the
    // stream graph generation.
    return new StreamGraphGenerator(
                    new ArrayList<>(transformations), config, checkpointCfg, configuration)
            .setStateBackend(defaultStateBackend)
            .setChangelogStateBackendEnabled(changelogStateBackendEnabled)
            .setSavepointDir(defaultSavepointDirectory)
            .setChaining(isChainingEnabled)
            .setUserArtifacts(cacheFile)
            .setTimeCharacteristic(timeCharacteristic)
            .setDefaultBufferTimeout(bufferTimeout)
            .setSlotSharingGroupResource(slotSharingGroupResources);
}
```

StreamGraphGenerator.generate();

```java
public StreamGraph generate() {
  //new StreamGraph 中保留config相关数据 然后有一个clear(); create an empty new stream graph.
    streamGraph = new StreamGraph(executionConfig, checkpointConfig, savepointRestoreSettings);
  //判断 RuntimeExecutionMode AUTOMATIC BATCH
    shouldExecuteInBatchMode = shouldExecuteInBatchMode();
  //设置了一些属性 默认和配置
    configureStreamGraph(streamGraph);

    alreadyTransformed = new IdentityHashMap<>();

    for (Transformation<?> transformation : transformations) {
      //transform(transformation); 是怎么和 streamGraph中streamNodes产生关系的呢?
      //streamGraph 是 StreamGraphGenerator内部的一个对象 所以为什么每次new StreamGraph()时需要clear();
        transform(transformation);
    }

    streamGraph.setSlotSharingGroupResource(slotSharingGroupResources);

    setFineGrainedGlobalStreamExchangeMode(streamGraph);
//streamGraph.getStreamNodes() 前面貌似没有 streamGraph 添加节点 哪里来的? 
  //streamGraph clear()中 streamNodes = new HashMap<>(); 是空的
  //只有一个 protected StreamNode addNode()方法有放入
    for (StreamNode node : streamGraph.getStreamNodes()) {
        if (node.getInEdges().stream().anyMatch(this::shouldDisableUnalignedCheckpointing)) {
            for (StreamEdge edge : node.getInEdges()) {
                edge.setSupportsUnalignedCheckpoints(false);
            }
        }
    }

    final StreamGraph builtStreamGraph = streamGraph;
// 清理转换过程中的数据
    alreadyTransformed.clear();
    alreadyTransformed = null;
    streamGraph = null;

    return builtStreamGraph;
}
```

transform(transformation);

```java
private Collection<Integer> transform(Transformation<?> transform) {
    if (alreadyTransformed.containsKey(transform)) {
        return alreadyTransformed.get(transform);//判断是否已经解析过 有就直接取
    }

    LOG.debug("Transforming " + transform);

    if (transform.getMaxParallelism() <= 0) {

        // if the max parallelism hasn't been set, then first use the job wide max parallelism
        // from the ExecutionConfig.
        int globalMaxParallelismFromConfig = executionConfig.getMaxParallelism();
        if (globalMaxParallelismFromConfig > 0) {
            transform.setMaxParallelism(globalMaxParallelismFromConfig);
        }
    }

    transform
            .getSlotSharingGroup()
            .ifPresent(
                    slotSharingGroup -> {
                        final ResourceSpec resourceSpec =
                                SlotSharingGroupUtils.extractResourceSpec(slotSharingGroup);
                        if (!resourceSpec.equals(ResourceSpec.UNKNOWN)) {
                            slotSharingGroupResources.compute(
                                    slotSharingGroup.getName(),
                                    (name, profile) -> {
                                        if (profile == null) {
                                            return ResourceProfile.fromResourceSpec(
                                                    resourceSpec, MemorySize.ZERO);
                                        } else if (!ResourceProfile.fromResourceSpec(
                                                        resourceSpec, MemorySize.ZERO)
                                                .equals(profile)) {
                                            throw new IllegalArgumentException(
                                                    "The slot sharing group "
                                                            + slotSharingGroup.getName()
                                                            + " has been configured with two different resource spec.");
                                        } else {
                                            return profile;
                                        }
                                    });
                        }
                    });

    // call at least once to trigger exceptions about MissingTypeInfo
    transform.getOutputType();

    @SuppressWarnings("unchecked")
    final TransformationTranslator<?, Transformation<?>> translator =
            (TransformationTranslator<?, Transformation<?>>)
                    translatorMap.get(transform.getClass());

    Collection<Integer> transformedIds;
    if (translator != null) {
        transformedIds = translate(translator, transform);//核心就是这里
    } else {
        transformedIds = legacyTransform(transform); //区分的目的是?
    }

    // need this check because the iterate transformation adds itself before
    // transforming the feedback edges
    if (!alreadyTransformed.containsKey(transform)) {
        alreadyTransformed.put(transform, transformedIds);
    }

    return transformedIds;
}
```

transformedIds = translate(translator, transform);

```java
private Collection<Integer> translate(
        final TransformationTranslator<?, Transformation<?>> translator,
        final Transformation<?> transform) {
    checkNotNull(translator);
    checkNotNull(transform);
//获取所有上游节点ID 这个地方进去看看 先不断的向前把所有的上游都解析后 才解析当前
    final List<Collection<Integer>> allInputIds = getParentInputIds(transform.getInputs());

    // the recursive call might have already transformed this
  //已解析直接返回
    if (alreadyTransformed.containsKey(transform)) {
        return alreadyTransformed.get(transform);
    }

    final String slotSharingGroup =
      //判断是否执行在一个slot组? SlotSharing 宽窄依赖?
            determineSlotSharingGroup(
                    transform.getSlotSharingGroup().isPresent()
                            ? transform.getSlotSharingGroup().get().getName()
                            : null,
                    allInputIds.stream()
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList()));

    final TransformationTranslator.Context context =
            new ContextImpl(this, streamGraph, slotSharingGroup, configuration);

    return shouldExecuteInBatchMode//批流分开 Sink和SimpleTransformation分开实现
      //translateForBatch  translateForStreaming 都有两个实现 SimpleTransformationTranslator SinkTransformationTranslator 
            ? translator.translateForBatch(transform, context)
            : translator.translateForStreaming(transform, context);
}
```

需要重点看下 getParentInputIds(transform.getInputs()); 递归向前的逻辑

```java
private List<Collection<Integer>> getParentInputIds(
        @Nullable final Collection<Transformation<?>> parentTransformations) {
    final List<Collection<Integer>> allInputIds = new ArrayList<>();
    if (parentTransformations == null) {
        return allInputIds;
    }

    for (Transformation<?> transformation : parentTransformations) {
      //先把上游都解析了 才将其ID返回
        allInputIds.add(transform(transformation));
    }
    return allInputIds;
}
```

SinkTransformationTranslator translator.translateForBatch(transform, context)  translator.translateForStreaming(transform, context)

```java
@Override
public Collection<Integer> translateForBatch(
        SinkTransformation<Input, Output> transformation, Context context) {
    return translateInternal(transformation, context, true);
}
```

```java
private Collection<Integer> translateInternal(
        SinkTransformation<Input, Output> transformation, Context context, boolean batch) {
    SinkExpander<Input> expander =
            new SinkExpander<>(
                    transformation.getInputStream(),
                    transformation.getSink(),
                    transformation,
                    context,
                    batch);
    expander.expand();//核心在这里
    return Collections.emptyList();
}
```

```java
private void expand() {

    final int sizeBefore = executionEnvironment.getTransformations().size();

    DataStream<T> prewritten = inputStream;
//if else里面都是对特殊 sink做了处理
    if (sink instanceof WithPreWriteTopology) {
        prewritten =
                adjustTransformations(
                        prewritten,
                        ((WithPreWriteTopology<T>) sink)::addPreWriteTopology,
                        true,
                        sink instanceof SupportsConcurrentExecutionAttempts);
    }

    if (sink instanceof TwoPhaseCommittingSink) {
        addCommittingTopology(sink, prewritten);
    } else {
        adjustTransformations(
                prewritten,
                input ->
                        input.transform(
                                WRITER_NAME,
                                CommittableMessageTypeInfo.noOutput(),
                                new SinkWriterOperatorFactory<>(sink)),
                false,
                sink instanceof SupportsConcurrentExecutionAttempts);
    }

    final List<Transformation<?>> sinkTransformations =
            executionEnvironment
                    .getTransformations()
                    .subList(sizeBefore, executionEnvironment.getTransformations().size());
    sinkTransformations.forEach(context::transform);//核心还是在这

    // Remove all added sink subtransformations to avoid duplications and allow additional
    // expansions 
    while (executionEnvironment.getTransformations().size() > sizeBefore) {
        executionEnvironment
                .getTransformations()
                .remove(executionEnvironment.getTransformations().size() - 1);
    }
}
```

sinkTransformations.forEach(context::transform); 又回到了 StreamGraphGenerator

```java
@Override
public Collection<Integer> transform(Transformation<?> transformation) {
    return streamGraphGenerator.transform(transformation);//后续又是回到之前的判断逻辑 是否 批流 sink等
}
```

SimpleTransformationTranslator translator.translateForBatch(transform, context) 

```java
@Override
public final Collection<Integer> translateForBatch(
        final T transformation, final Context context) {
    checkNotNull(transformation);
    checkNotNull(context);

    final Collection<Integer> transformedIds =
            translateForBatchInternal(transformation, context);
    configure(transformation, context);

    return transformedIds;
}
```

```java
protected abstract Collection<Integer> translateForBatchInternal(
        final T transformation, final Context context);
```

translator.translateForStreaming(transform, context)

```java
@Override
public final Collection<Integer> translateForStreaming(
        final T transformation, final Context context) {
    checkNotNull(transformation);
    checkNotNull(context);

    final Collection<Integer> transformedIds =
            translateForStreamingInternal(transformation, context);
    configure(transformation, context);

    return transformedIds;
}
```

```java
protected abstract Collection<Integer> translateForStreamingInternal(
        final T transformation, final Context context);
```

translateForBatchInternal translateForStreamingInternal 都有一堆的实现类 以OneInputTransformationTranslator为例看看

```java
@Override
public Collection<Integer> translateForBatchInternal(
        final OneInputTransformation<IN, OUT> transformation, final Context context) {
    KeySelector<IN, ?> keySelector = transformation.getStateKeySelector();
    Collection<Integer> ids =
            translateInternal(
                    transformation,
                    transformation.getOperatorFactory(),
                    transformation.getInputType(),
                    keySelector,
                    transformation.getStateKeyType(),
                    context);
    boolean isKeyed = keySelector != null;
    if (isKeyed) {
        BatchExecutionUtils.applyBatchExecutionSettings(
                transformation.getId(), context, StreamConfig.InputRequirement.SORTED);
    }

    return ids;
}
```

```java
@Override
public Collection<Integer> translateForStreamingInternal(
        final OneInputTransformation<IN, OUT> transformation, final Context context) {
    return translateInternal(
            transformation,
            transformation.getOperatorFactory(),
            transformation.getInputType(),
            transformation.getStateKeySelector(),
            transformation.getStateKeyType(),
            context);
}
```

然后都是调用 AbstractOneInputTransformationTranslator.translateInternal()

```java
protected Collection<Integer> translateInternal(
        final Transformation<OUT> transformation,
        final StreamOperatorFactory<OUT> operatorFactory,
        final TypeInformation<IN> inputType,
        @Nullable final KeySelector<IN, ?> stateKeySelector,
        @Nullable final TypeInformation<?> stateKeyType,
        final Context context) {
    checkNotNull(transformation);
    checkNotNull(operatorFactory);
    checkNotNull(inputType);
    checkNotNull(context);

    final StreamGraph streamGraph = context.getStreamGraph();
    final String slotSharingGroup = context.getSlotSharingGroup();
    final int transformationId = transformation.getId();
    final ExecutionConfig executionConfig = streamGraph.getExecutionConfig();
//调用StreamGraph.addOperator()方法将Transformation中的 OperatorFactory添加到StreamGraph中。
    streamGraph.addOperator(
            transformationId,
            slotSharingGroup,
            transformation.getCoLocationGroupKey(),
            operatorFactory,
            inputType,
            transformation.getOutputType(),
            transformation.getName());
//调用streamGraph.setOneInputStateKey()方法设定 KeySelector参数信息。
    if (stateKeySelector != null) {
        TypeSerializer<?> keySerializer = stateKeyType.createSerializer(executionConfig);
        streamGraph.setOneInputStateKey(transformationId, stateKeySelector, keySerializer);
    }
//获得Transformation中的并行度参数，并将其设置到 StreamGraph中。
    int parallelism =
            transformation.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT
                    ? transformation.getParallelism()
                    : executionConfig.getParallelism();
    streamGraph.setParallelism(
            transformationId, parallelism, transformation.isParallelismConfigured());
    streamGraph.setMaxParallelism(transformationId, transformation.getMaxParallelism());

    final List<Transformation<?>> parentTransformations = transformation.getInputs();
    checkState(
            parentTransformations.size() == 1,
            "Expected exactly one input transformation but found "
                    + parentTransformations.size());
//调用streamGraph.addEdge方法，将上游转换操作的inputId和 当前转换操作的transformId相连，构建成StreamGraph对应的边。
    for (Integer inputId : context.getStreamNodeIds(parentTransformations.get(0))) {
        streamGraph.addEdge(inputId, transformationId, 0);
    }
// parentTransformations 只取了ID构建边 没有进一步向前递归 transform? 回头看final List<Collection<Integer>> allInputIds = getParentInputIds(transform.getInputs());
    if (transformation instanceof PhysicalTransformation) {
        streamGraph.setSupportsConcurrentExecutionAttempts(
                transformationId,
                ((PhysicalTransformation<OUT>) transformation)
                        .isSupportsConcurrentExecutionAttempts());
    }
//返回当前Transformation的ID。
    return Collections.singleton(transformationId);
}
```

提交 StreamExecutionEnvironment.executeAsync(streamGraph) 转 JobGraph