Transformation负责描述DataStream之间的转换 信息，而Transformation结构中最主要的组成部分就是 StreamOperator

StreamOperator作为接口，在被OneInputStreamOperator接口和 TwoInputStreamOperator接口继承的同时，又分别被 AbstractStreamOperator和AbstractUdfStreamOperator两个抽象类继 承和实现。其中OneInputStreamOperator和TwoInputStreamOperator 定义了不同输入数量的StreamOperator方法，例如:单输入类型算子 通常会实现OneInputStreamOperator接口，常见的实现有 StreamSource和StreamSink等算子;TwoInputStreamOperator则定义 了双输入类型算子，常见的实现有CoProcessOperator、CoStreamMap 等算子。从这里我们可以看出，StreamOperator和Transformation基 本上是一一对应的，最多支持双输入类型算子，而不支持多输入类 型，用户可以通过多次关联TwoInputTransformation实现多输入类型 的算子。

不管是OneInputStreamOperator还是 TwoInputStreamOperator类型的算子，最终都会继承 AbstractStreamOperator基本实现类。在调度和执行Task实例时，会 通过AbstractStreamOperator提供的入口方法触发和执行Operator。 同时在AbstractStreamOperator中也定义了所有算子中公共的组成部 分，如StreamingRuntimeContext、OperatorStateBackend等。对于 AbstractStreamOperator如何被SubTask触发和执行.另外， AbstractUdfStreamOperator基本实现类则主要包含了UserFunction成 员变量，允许当前算子通过自定义UserFunction实现具体的计算逻 辑。

StreamOperator接口实现的方法主要供Task调用和执行。

```java
/**
 * Basic interface for stream operators. Implementers would implement one of {@link
 * org.apache.flink.streaming.api.operators.OneInputStreamOperator} or {@link
 * org.apache.flink.streaming.api.operators.TwoInputStreamOperator} to create operators that process
 * elements.
 *
 * <p>The class {@link org.apache.flink.streaming.api.operators.AbstractStreamOperator} offers
 * default implementation for the lifecycle and properties methods.
 *
 * <p>Methods of {@code StreamOperator} are guaranteed not to be called concurrently. Also, if using
 * the timer service, timer callbacks are also guaranteed not to be called concurrently with methods
 * on {@code StreamOperator}.
 *
 * @param <OUT> The output type of the operator
 */
@PublicEvolving
public interface StreamOperator<OUT> extends CheckpointListener, KeyContext, Serializable {

    // ------------------------------------------------------------------------
    //  life cycle
    // ------------------------------------------------------------------------

    /**
     * This method is called immediately before any elements are processed, it should contain the
     * operator's initialization logic.
     *
     * @implSpec In case of recovery, this method needs to ensure that all recovered data is
     *     processed before passing back control, so that the order of elements is ensured during
     *     the recovery of an operator chain (operators are opened from the tail operator to the
     *     head operator).
     * @throws java.lang.Exception An exception in this method causes the operator to fail.
     */
  //定义当前Operator的初始化方法，在数据元素正式接入 Operator运算之前，Task会调用StreamOperator.open()方法对该算子进行 初始化，具体open()方法的定义由子类实现，常见的用法如调用 RichFunction中的open()方法创建相应的状态变量。
    void open() throws Exception;

    /**
     * This method is called at the end of data processing.
     *
     * <p>The method is expected to flush all remaining buffered data. Exceptions during this
     * flushing of buffered data should be propagated, in order to cause the operation to be
     * recognized as failed, because the last data items are not processed properly.
     *
     * <p><b>After this method is called, no more records can be produced for the downstream
     * operators.</b>
     *
     * <p><b>WARNING:</b> It is not safe to use this method to commit any transactions or other side
     * effects! You can use this method to flush any buffered data that can later on be committed
     * e.g. in a {@link StreamOperator#notifyCheckpointComplete(long)}.
     *
     * <p><b>NOTE:</b>This method does not need to close any resources. You should release external
     * resources in the {@link #close()} method.
     *
     * @throws java.lang.Exception An exception in this method causes the operator to fail.
     */
  //当所有的数据元素都添加到当前Operator时，就会调用 该方法刷新所有剩余的缓冲数据，保证算子中所有数据被正确处理。
    void finish() throws Exception;

    /**
     * This method is called at the very end of the operator's life, both in the case of a
     * successful completion of the operation, and in the case of a failure and canceling.
     *
     * <p>This method is expected to make a thorough effort to release all resources that the
     * operator has acquired.
     *
     * <p><b>NOTE:</b>It can not emit any records! If you need to emit records at the end of
     * processing, do so in the {@link #finish()} method.
     */
  //算子生命周期结束时会调用此方法，包括算子操作执 行成功、失败或者取消时。
    void close() throws Exception;

    // ------------------------------------------------------------------------
    //  state snapshots
    // ------------------------------------------------------------------------

    /**
     * This method is called when the operator should do a snapshot, before it emits its own
     * checkpoint barrier.
     *
     * <p>This method is intended not for any actual state persistence, but only for emitting some
     * data before emitting the checkpoint barrier. Operators that maintain some small transient
     * state that is inefficient to checkpoint (especially when it would need to be checkpointed in
     * a re-scalable way) but can simply be sent downstream before the checkpoint. An example are
     * opportunistic pre-aggregation operators, which have small the pre-aggregation state that is
     * frequently flushed downstream.
     *
     * <p><b>Important:</b> This method should not be used for any actual state snapshot logic,
     * because it will inherently be within the synchronous part of the operator's checkpoint. If
     * heavy work is done within this method, it will affect latency and downstream checkpoint
     * alignments.
     *
     * @param checkpointId The ID of the checkpoint.
     * @throws Exception Throwing an exception here causes the operator to fail and go into
     *     recovery.
     */
  //在StreamOperator正式执行checkpoint 操作之前会调用该方法
    void prepareSnapshotPreBarrier(long checkpointId) throws Exception;

    /**
     * Called to draw a state snapshot from the operator.
     *
     * @return a runnable future to the state handle that points to the snapshotted state. For
     *     synchronous implementations, the runnable might already be finished.
     * @throws Exception exception that happened during snapshotting.
     */
  //当SubTask执行checkpoint操作时会调用该方法， 用于触发该Operator中状态数据的快照操作。
    OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory storageLocation)
            throws Exception;

    /** Provides a context to initialize all state in the operator. */
  //当算子启动或重启时，调用该方法初始化状态数 据，当恢复作业任务时，算子会从检查点(checkpoint)持久化的数据 中恢复状态数据。
    void initializeState(StreamTaskStateInitializer streamTaskStateManager) throws Exception;

    // ------------------------------------------------------------------------
    //  miscellaneous
    // ------------------------------------------------------------------------

    void setKeyContextElement1(StreamRecord<?> record) throws Exception;

    void setKeyContextElement2(StreamRecord<?> record) throws Exception;

    OperatorMetricGroup getMetricGroup();

    OperatorID getOperatorID();
}
```

AbstractStreamOperator作为StreamOperator的基本实现类，所 有的Operator都会继承和实现该抽象实现类。在 AbstractStreamOperator中定义了Operator用到的基础方法和成员信息

```java
/**
 * Base class for all stream operators. Operators that contain a user function should extend the
 * class {@link AbstractUdfStreamOperator} instead (which is a specialized subclass of this class).
 *
 * <p>For concrete implementations, one of the following two interfaces must also be implemented, to
 * mark the operator as unary or binary: {@link OneInputStreamOperator} or {@link
 * TwoInputStreamOperator}.
 *
 * <p>Methods of {@code StreamOperator} are guaranteed not to be called concurrently. Also, if using
 * the timer service, timer callbacks are also guaranteed not to be called concurrently with methods
 * on {@code StreamOperator}.
 *
 * <p>Note, this class is going to be removed and replaced in the future by {@link
 * AbstractStreamOperatorV2}. However as {@link AbstractStreamOperatorV2} is currently experimental,
 * {@link AbstractStreamOperator} has not been deprecated just yet.
 *
 * @param <OUT> The output type of the operator.
 */
@PublicEvolving
public abstract class AbstractStreamOperator<OUT>
        implements StreamOperator<OUT>,
                SetupableStreamOperator<OUT>,
                CheckpointedStreamOperator,
                KeyContextHandler,
                Serializable {
    private static final long serialVersionUID = 1L;

    /** The logger used by the operator class and its subclasses. */
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractStreamOperator.class);

    // ----------- configuration properties -------------

    // A sane default for most operators
    //用于指定Operator的上下游算子 链接策略，其中ChainStrategy可以是ALWAYS、NEVER或HEAD等类型， 该参数实际上就是转换过程中配置的链接策略。默认是HEAD?不应该是ALWAYS么
    protected ChainingStrategy chainingStrategy = ChainingStrategy.HEAD;

    // ---------------- runtime fields ------------------

    /** The task that contains this operator (and other operators in the same chain). */
    //表示当前Operator所属的 StreamTask，最终会通过StreamTask中的invoke()方法执行当前StreamTask 中的所有Operator。
    private transient StreamTask<?, ?> container;
//存储了该StreamOperator的配置信息，实际 上是对Configuration参数进行了封装。
    protected transient StreamConfig config;
//定义了当前 StreamOperator的输出操作，执行完该算子的所有转换操作后，会通过 Output组件将数据推送到下游算子继续执行。
    protected transient Output<StreamRecord<OUT>> output;

    private transient IndexedCombinedWatermarkStatus combinedWatermark;

    /** The runtime context for UDFs. */
   //主要定义了UDF执行过 程中的上下文信息，例如获取累加器、状态数据。
    private transient StreamingRuntimeContext runtimeContext;

    // ---------------- key/value state ------------------

    /**
     * {@code KeySelector} for extracting a key from an element being processed. This is used to
     * scope keyed state to a key. This is null if the operator is not a keyed operator.
     *
     * <p>This is for elements from the first input.
     */
    //只有DataStream经过keyBy()转 换操作生成KeyedStream后，才会设定该算子的stateKeySelector1变量信 息。
    private transient KeySelector<?, ?> stateKeySelector1;

    /**
     * {@code KeySelector} for extracting a key from an element being processed. This is used to
     * scope keyed state to a key. This is null if the operator is not a keyed operator.
     *
     * <p>This is for elements from the second input.
     */
     //只在执行两个KeyedStream关 联操作时使用，例如Join操作，在AbstractStreamOperator中会保存 stateKeySelector2的信息。
    private transient KeySelector<?, ?> stateKeySelector2;
//Class encapsulating various state backend handling logic for 管理StreamOperatorState
    private transient StreamOperatorStateHandler stateHandler;
//Flink内部时间 服务，和processingTimeService相似，但支持基于事件时间的时间域处 理数据，还可以同时注册基于事件时间和处理时间的定时器，例如在 窗口、CEP等高级类型的算子中，会在ProcessFunction中通过 timeServiceManager注册Timer定时器，当事件时间或处理时间到达指定 时间后执行Timer定时器，以实现复杂的函数计算。
    private transient InternalTimeServiceManager<?> timeServiceManager;

    // --------------- Metrics ---------------------------

    /** Metric group for the operator. */
   //用于记录当前算子层面的监控指 标，包括numRecordsIn、numRecordsOut、numRecordsInRate、 numRecordsOutRate等。
    protected transient InternalOperatorMetricGroup metrics;
//用于采集和汇报当前Operator的延时状 况。
    protected transient LatencyStats latencyStats;

    // ---------------- time handler ------------------
//基于ProcessingTime 的时间服务，实现ProcessingTime时间域操作，例如获取当前 ProcessingTime，然后创建定时器回调等。
    protected transient ProcessingTimeService processingTimeService;
}
```

StreamOperatorStateHandler

```java
@Internal
public class StreamOperatorStateHandler {

    protected static final Logger LOG = LoggerFactory.getLogger(StreamOperatorStateHandler.class);

    /** Backend for keyed state. This might be empty if we're not on a keyed stream. */
  //用于存储 KeyedState的状态管理后端
    @Nullable private final CheckpointableKeyedStateBackend<?> keyedStateBackend;

    private final CloseableRegistry closeableRegistry;
  //主要提供KeyedState的状 态存储服务，实际上是对KeyedStateBackend进行封装并提供了不同类型 的KeyedState获取方法，例如通过 getReducingState(ReducingStateDescriptor stateProperties)方法获取 ReducingState。
    @Nullable private final DefaultKeyedStateStore keyedStateStore;
    private final OperatorStateBackend operatorStateBackend;
    private final StreamOperatorStateContext context;
```

AbstractUdfStreamOperator基本实现

当StreamOperator涉及自定义用户函数数据转换处理时，对应的 Operator会继承AbstractUdfStreamOperator抽象实现类，常见的有 StreamMap、CoProcessOperator等算子。当然，并不是所有的 Operator都继承自AbstractUdfStreamOperator。在Flink Table API 模块实现的算子中，都会直接继承和实现AbstractStreamOperator抽 象实现类。另外，有状态查询的AbstractQueryableStateOperator也 不需要使用用户自定义函数处理数据。

AbstractUdfStreamOperator继承自AbstractStreamOperator抽象 类，对于AbstractUdfStreamOperator抽象类来讲，最重要的拓展就是 增加了成员变量userFunction，且提供了userFunction初始化以及状 态持久化的抽象方法。下面我们简单介绍AbstractUdfStreamOperator 提供的主要方法。

AbstractUdfStreamOperator

```java
/**
 * This is used as the base class for operators that have a user-defined function. This class
 * handles the opening and closing of the user-defined functions, as part of the operator life
 * cycle.
 *
 * @param <OUT> The output type of the operator
 * @param <F> The type of the user function
 */
@PublicEvolving
public abstract class AbstractUdfStreamOperator<OUT, F extends Function>
        extends AbstractStreamOperator<OUT>
        implements OutputTypeConfigurable<OUT>, UserFunctionProvider<F> {

    private static final long serialVersionUID = 1L;

    /** The user function. */
    protected final F userFunction;

    public AbstractUdfStreamOperator(F userFunction) {
        this.userFunction = requireNonNull(userFunction);
        checkUdfCheckpointingPreconditions();
    }

    /**
     * Gets the user function executed in this operator.
     *
     * @return The user function of this operator.
     */
    public F getUserFunction() {
        return userFunction;
    }

    // ------------------------------------------------------------------------
    //  operator life cycle
    // ------------------------------------------------------------------------
//FunctionUtils为userFunction设定RuntimeContext变量。 此时userFunction能够获取RuntimeContext变量，然后实现获取状态 数据等操作。
    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
        FunctionUtils.setFunctionRuntimeContext(userFunction, getRuntimeContext());
    }
//StreamingFunctionUtils.snapshotFunctionState()方法，以实现对 userFunction中的状态进行快照操作。
    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        StreamingFunctionUtils.snapshotFunctionState(
                context, getOperatorStateBackend(), userFunction);
    }
//StreamingFunctionUtils.restoreFunctionState()方法初始化 userFunction的状态值。
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        StreamingFunctionUtils.restoreFunctionState(context, userFunction);
    }
//当用户自定义并实现 RichFunction时，FunctionUtils.openFunction()方法会调用 RichFunction.open()方法，完成用户自定义状态的创建和初始化。
    @Override
    public void open() throws Exception {
        super.open();
        FunctionUtils.openFunction(userFunction, new Configuration());
    }

    @Override
    public void finish() throws Exception {
        super.finish();
        if (userFunction instanceof SinkFunction) {
            ((SinkFunction<?>) userFunction).finish();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        FunctionUtils.closeFunction(userFunction);
    }

    // ------------------------------------------------------------------------
    //  checkpointing and recovery
    // ------------------------------------------------------------------------

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);

        if (userFunction instanceof CheckpointListener) {
            ((CheckpointListener) userFunction).notifyCheckpointComplete(checkpointId);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);

        if (userFunction instanceof CheckpointListener) {
            ((CheckpointListener) userFunction).notifyCheckpointAborted(checkpointId);
        }
    }

    // ------------------------------------------------------------------------
    //  Output type configuration
    // ------------------------------------------------------------------------

    @Override
    public void setOutputType(TypeInformation<OUT> outTypeInfo, ExecutionConfig executionConfig) {
        StreamingFunctionUtils.setOutputType(userFunction, outTypeInfo, executionConfig);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Since the streaming API does not implement any parametrization of functions via a
     * configuration, the config returned here is actually empty.
     *
     * @return The user function parameters (currently empty)
     */
    public Configuration getUserFunctionParameters() {
        return new Configuration();
    }

    private void checkUdfCheckpointingPreconditions() {

        if (userFunction instanceof CheckpointedFunction
                && userFunction instanceof ListCheckpointed) {

            throw new IllegalStateException(
                    "User functions are not allowed to implement "
                            + "CheckpointedFunction AND ListCheckpointed.");
        }
    }
}

```

可以看出，当用户自定义实现Function时，在 AbstractUdfStreamOperator抽象类中提供了对这些Function的初始化 操作，也就实现了Operator和Function之间的关联。Operator也是 Function的载体，具体数据处理操作借助Operator中的Function进 行。StreamOperator提供了执行Function的环境，包括状态数据管理 和处理Watermark、LatencyMarker等信息。

StreamOperator根据输入流的数量分为两种类型，即支持单输入 流的OneInputStreamOperator以及支持双输入流的 TwoInputStreamOperator，我们可以将其称为一元输入算子和二元输 入算子

OneInputStreamOperator的实现

OneInputStreamOperator定义了单输入流的StreamOperator，常 见的实现类有StreamMap、StreamFilter等算子。

```java
/**
 * Interface for stream operators with one input. Use {@link
 * org.apache.flink.streaming.api.operators.AbstractStreamOperator} as a base class if you want to
 * implement a custom operator.
 *
 * @param <IN> The input type of the operator
 * @param <OUT> The output type of the operator
 */
@PublicEvolving
public interface OneInputStreamOperator<IN, OUT> extends StreamOperator<OUT>, Input<IN> {
    @Override
    default void setKeyContextElement(StreamRecord<IN> record) throws Exception {
        setKeyContextElement1(record);
    }
}
```

StreamFilter

```java
/** A {@link StreamOperator} for executing {@link FilterFunction FilterFunctions}. */
@Internal
public class StreamFilter<IN> extends AbstractUdfStreamOperator<IN, FilterFunction<IN>>
        implements OneInputStreamOperator<IN, IN> {

    private static final long serialVersionUID = 1L;
// 初始化FilterFunction并设定ChainingStrategy.ALWAYS
    public StreamFilter(FilterFunction<IN> filterFunction) {
        super(filterFunction);
        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
      // 执行userFunction.filter()方法
        if (userFunction.filter(element.getValue())) {
            output.collect(element);
        }
    }
}
```

TwoInputStreamOperator的实现

```java
/**
 * Interface for stream operators with two inputs. Use {@link
 * org.apache.flink.streaming.api.operators.AbstractStreamOperator} as a base class if you want to
 * implement a custom operator.
 *
 * @param <IN1> The input type of the operator
 * @param <IN2> The input type of the operator
 * @param <OUT> The output type of the operator
 */
@PublicEvolving
public interface TwoInputStreamOperator<IN1, IN2, OUT> extends StreamOperator<OUT> {

    /**
     * Processes one element that arrived on the first input of this two-input operator. This method
     * is guaranteed to not be called concurrently with other methods of the operator.
     */
  // 处理输入源1的数据元素方法
    void processElement1(StreamRecord<IN1> element) throws Exception;

    /**
     * Processes one element that arrived on the second input of this two-input operator. This
     * method is guaranteed to not be called concurrently with other methods of the operator.
     */
  // 处理输入源2的数据元素方法
    void processElement2(StreamRecord<IN2> element) throws Exception;

    /**
     * Processes a {@link Watermark} that arrived on the first input of this two-input operator.
     * This method is guaranteed to not be called concurrently with other methods of the operator.
     *
     * @see org.apache.flink.streaming.api.watermark.Watermark
     */
  // 处理输入源1的Watermark方法
    void processWatermark1(Watermark mark) throws Exception;

    /**
     * Processes a {@link Watermark} that arrived on the second input of this two-input operator.
     * This method is guaranteed to not be called concurrently with other methods of the operator.
     *
     * @see org.apache.flink.streaming.api.watermark.Watermark
     */
  // 处理输入源2的Watermark方法
    void processWatermark2(Watermark mark) throws Exception;

    /**
     * Processes a {@link LatencyMarker} that arrived on the first input of this two-input operator.
     * This method is guaranteed to not be called concurrently with other methods of the operator.
     *
     * @see org.apache.flink.streaming.runtime.streamrecord.LatencyMarker
     */
  // 处理输入源1的LatencyMarker方法
    void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception;

    /**
     * Processes a {@link LatencyMarker} that arrived on the second input of this two-input
     * operator. This method is guaranteed to not be called concurrently with other methods of the
     * operator.
     *
     * @see org.apache.flink.streaming.runtime.streamrecord.LatencyMarker
     */
  // 处理输入源2的LatencyMarker方法
    void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception;

    /**
     * Processes a {@link WatermarkStatus} that arrived on the first input of this two-input
     * operator. This method is guaranteed to not be called concurrently with other methods of the
     * operator.
     *
     * @see org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus
     */
    void processWatermarkStatus1(WatermarkStatus watermarkStatus) throws Exception;

    /**
     * Processes a {@link WatermarkStatus} that arrived on the second input of this two-input
     * operator. This method is guaranteed to not be called concurrently with other methods of the
     * operator.
     *
     * @see org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus
     */
    void processWatermarkStatus2(WatermarkStatus watermarkStatus) throws Exception;
}
```

TwoInputStreamOperator算子的具体实现 以CoStreamMap为例 CoStreamMap继承AbstractUdfStreamOperator的同时，实 现了TwoInputStreamOperator接口。其中在processElement1()和 processElement2()两个方法的实现中，分别调用了用户定义的 CoMapFunction的map1()和map2()方法对输入的数据元素Input1和 Input2进行处理。经过函数处理后的结果会通过output.collect()接 口推送到下游的Operator中。

```java
/**
 * {@link org.apache.flink.streaming.api.operators.StreamOperator} for processing {@link
 * CoMapFunction CoMapFunctions}.
 */
@Internal
public class CoStreamMap<IN1, IN2, OUT>
        extends AbstractUdfStreamOperator<OUT, CoMapFunction<IN1, IN2, OUT>>
        implements TwoInputStreamOperator<IN1, IN2, OUT> {

    private static final long serialVersionUID = 1L;

    public CoStreamMap(CoMapFunction<IN1, IN2, OUT> mapper) {
        super(mapper);
    }

    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        output.collect(element.replace(userFunction.map1(element.getValue())));
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        output.collect(element.replace(userFunction.map2(element.getValue())));
    }
}
```

StreamOperatorFactory

```java
/**
 * A factory to create {@link StreamOperator}.
 *
 * @param <OUT> The output type of the operator
 */
@PublicEvolving
public interface StreamOperatorFactory<OUT> extends Serializable {

    /** Create the operator. Sets access to the context and the output. */
    <T extends StreamOperator<OUT>> T createStreamOperator(
            StreamOperatorParameters<OUT> parameters);

    /** Set the chaining strategy for operator factory. */
    void setChainingStrategy(ChainingStrategy strategy);

    /** Get the chaining strategy of operator factory. */
    ChainingStrategy getChainingStrategy();

    /** Is this factory for {@link StreamSource}. */
    default boolean isStreamSource() {
        return false;
    }

    default boolean isLegacySource() {
        return false;
    }

    /**
     * If the stream operator need access to the output type information at {@link StreamGraph}
     * generation. This can be useful for cases where the output type is specified by the returns
     * method and, thus, after the stream operator has been created.
     */
    default boolean isOutputTypeConfigurable() {
        return false;
    }

    /**
     * Is called by the {@link StreamGraph#addOperator} method when the {@link StreamGraph} is
     * generated. The method is called with the output {@link TypeInformation} which is also used
     * for the {@link StreamTask} output serializer.
     *
     * @param type Output type information of the {@link StreamTask}
     * @param executionConfig Execution configuration
     */
    default void setOutputType(TypeInformation<OUT> type, ExecutionConfig executionConfig) {}

    /** If the stream operator need to be configured with the data type they will operate on. */
    default boolean isInputTypeConfigurable() {
        return false;
    }

    /**
     * Is called by the {@link StreamGraph#addOperator} method when the {@link StreamGraph} is
     * generated.
     *
     * @param type The data type of the input.
     * @param executionConfig The execution config for this parallel execution.
     */
    default void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {}

    /** Returns the runtime class of the stream operator. */
    Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader);
}
```

StreamOperator最终会通过 StreamOperatorFactory封装在Transformation结构中，并存储在 StreamGraph和JobGraph结构中，直到运行时执行StreamTask时，才会 调用StreamOperatorFactory.createStreamOperator()方法在 StreamOperatorFactory中定义StreamOperator实例。

通过StreamOperatorFactory封装创建StreamOperator的操作，在 DataStreamAPI中主要通过SimpleStreamOperatorFactory创建已经定 义的Operator，而在Table API模块中主要通过 CodeGenOperatorFactory从代码中动态编译并创建Operator实例。 SimpleStreamOperatorFactory和CodeGenOperatorFactory都是 StreamOperatorFactory的实现类。

StreamOperatorFactory接口定义了创建 StreamOperator的方法，并提供了设定ChainingStrategy、InputType 等属性的方法。

DataStream API中大部分转换操作都是通过 SimpleOperatorFactory进行封装和创建的。 SimpleStreamOperatorFactory根据算子类型的不同，拓展出了 InputFormatOperatorFactory、UdfStreamOperatorFactory和 OutputFormatOperatorFactory CollectSinkOperatorFactory四种接口实现。

·InputFormatOperatorFactory:支持创建InputFormat类型输入的 StreamSource算子，即SourceFunction为InputFormatSourceFunction类型， 并提供getInputFormat()方法生成StreamGraph。

·UdfStreamOperatorFactory:支持AbstractUdfStreamOperator类型的 Operator创建，并且在UdfStreamOperatorFactory中提供了获取 UserFunction的方法。

·OutputFormatOperatorFactory:支持创建OutputFormat类型输出 的StreamSink算子，即SinkFunction为OutputFormatSinkFunction类型，并 提供getOutputFormat()方法生成StreamGraph。

从SimpleOperatorFactory.of()方法定义 中可以看出，基于StreamOperator提供的of()方法对算子进行工厂类 的封装，实现将Operator封装在OperatorFactory中。然后根据 Operator类型的不同，创建不同的SimpleOperatorFactory实现类，例 如当Operator类型为StreamSource且UserFunction定义属于 InputFormatSourceFunction时，就会创建 SimpleInputFormatOperatorFactory实现类，其他情况类似。

SimpleOperatorFactory.of()

```java
/** Create a SimpleOperatorFactory from existed StreamOperator. */
@SuppressWarnings("unchecked")
public static <OUT> SimpleOperatorFactory<OUT> of(StreamOperator<OUT> operator) {
    if (operator == null) {
        return null;
    } else if (operator instanceof StreamSource
            && ((StreamSource) operator).getUserFunction()
                    instanceof InputFormatSourceFunction) {
      // 如果Operator是StreamSource类型，且UserFunction类型为InputFormatSourceFunction
     // 返回SimpleInputFormatOperatorFactory
        return new SimpleInputFormatOperatorFactory<OUT>((StreamSource) operator);
    } else if (operator instanceof UserFunctionProvider
            && (((UserFunctionProvider<Function>) operator).getUserFunction()
                    instanceof OutputFormatSinkFunction)) {
      // 如果Operator是StreamSink类型，且UserFunction类型为OutputFormatSinkFunction
// 返回SimpleOutputFormatOperatorFactory
        return new SimpleOutputFormatOperatorFactory<>(
                (((OutputFormatSinkFunction<?>)
                                ((UserFunctionProvider<Function>) operator).getUserFunction())
                        .getFormat()),
                operator);
    } else if (operator instanceof AbstractUdfStreamOperator) {
      // 如果Operator是AbstractUdfStreamOperator则返回
        return new SimpleUdfStreamOperatorFactory<OUT>((AbstractUdfStreamOperator) operator);
    } else {
      // 其他情况返回SimpleOperatorFactory
        return new SimpleOperatorFactory<>(operator);
    }
}
```

在集群中执行该算子时，首先会调用 SimpleOperatorFactory.createStreamOperator()方法创建 StreamOperator实例。如果算子同时实现了SetupableStreamOperator 接口，则会调用setup()方法对算子进行基本的设置。 SetupableStreamOperator已经标注过期

SimpleOperatorFactory.createStreamOperator

```
@Override
public <T extends StreamOperator<OUT>> T createStreamOperator(
        StreamOperatorParameters<OUT> parameters) {
    if (operator instanceof AbstractStreamOperator) {
        ((AbstractStreamOperator) operator).setProcessingTimeService(processingTimeService);
    }
    if (operator instanceof SetupableStreamOperator) {
        ((SetupableStreamOperator) operator)
                .setup(
                        parameters.getContainingTask(),
                        parameters.getStreamConfig(),
                        parameters.getOutput());
    }
    return (T) operator;
}
```