DataStream转换操作中的数据处理逻辑主要是通 过自定义函数实现的，Function作为Flink中最小的数据处理单元，在 Flink中占据非常重要的地位。和Java提供的Function接口类似， Flink实现的Function接口专门用于处理接入的数据元素。 StreamOperator负责对内部Function的调用和执行，当 StreamOperator被Task调用和执行时，StreamOperator会将接入的数 据元素传递给内部Function进行处理，然后将Function处理后的结果 推送给下游的算子继续处理。

```java
/**
 * The base interface for all user-defined functions.
 *
 * <p>This interface is empty in order to allow extending interfaces to be SAM (single abstract
 * method) interfaces that can be implemented via Java 8 lambdas.
 */
@Public
public interface Function extends java.io.Serializable {}
```

Function接口是所有自定义函数的父类， MapFunction和FlatMapFunction都是直接继承自Function接口，并提 供各自的数据处理方法。其中MapFunction接口提供了map()方法实现 数据的一对一转换处理，FlatMapFunction提供了flatMap()方法实现 对输入数据元素的一对多转换，即输入一条数据产生多条输出结果。 在flatMap()方法中通过Collector接口实现了对输出结果的收集操 作。当然还有其他类型的Function实现，例如FilterFunction

RichFunction接口实现对有 状态计算的支持，RichFunction接口除了包含open()和close()方法之外，还提供了获取RuntimeContext的方法，并在 AbstractRichFunction抽象类类中提供了对RichFunction接口的基本 实现。RichMapFunction和RichFlatMapFunction接口实现类最终通过 AbstractRichFunction提供的getRuntimeContext()方法获取 RuntimeContext对象，进而操作状态数据。

用户可以选择相应的Function接口实现不同类型的业务转换逻 辑，例如MapFunction接口中提供的map()方法可以实现数据元素的一 对一转换。对于普通的Function转换没有太多需要展开的内容，接下 来我们重点了解RichFunction的实现细节，具体了解Flink中如何通过 RichFunction实现有状态计算。

RichFunction详解

RichFunction接口实际上对Function进行了补充和拓展，提供了 控制函数生命周期的open()和close()方法，所有实现了RichFunction 的子类都能够获取RuntimeContext对象。而RuntimeContext包含了算 子执行过程中所有运行时的上下文信息，例如Accumulator、 BroadcastVariable和DistributedCache等变量。

RuntimeContext上下文 RuntimeContext接口定义了非常丰富的方法，例如 创建和获取Accumulator、BroadcastVariable变量的方法以及在状态 操作过程中使用到的getState()和getListState()等方法。

不同类型的Operator创建的RuntimeContext也有一定区别，因此 在Flink中提供了不同的RuntimeContext实现类，以满足不同Operator 对运行时上下文信息的获取。其中AbstractRuntimeUDFContext主要用 于获取提供UDF函数的相关运行时上下文信息，且 AbstractRuntimeUDFContext又分别被RuntimeUDFContext、 DistributedRuntimeUDFContext以及StreamingRuntimeContext三个子 类继承和实现。RuntimeUDFContext主要用于CollectionExecutor; DistributedRuntimeUDFContext则主要用于BatchTask、DataSinkTask 以及DataSourceTask等离线场景。流式数据处理中使用最多的是 StreamingRuntimeContext。

当然还有其他场景使用到的RuntimeContext实现类，例如 CepRuntimeContext、SavepointRuntimeContext以及 IterationRuntimeContext，这些RuntimeContext实现类主要服务于相 应类型的数据处理场景，

自定义RichMapFunction实例重写open()方法中，可以调用getRuntimeContext()方法获 取RuntimeContext，这里的RuntimeContext实际上就是前面提到的 StreamingRuntimeContext对象。接下来使用RuntimeContext提供的接 口方法获取运行时上下文信息。例如获取MetricGroup创建Counter指 标累加器以及调用getState()方法创建ValueState。最后创建好的 Metric和ValueState都可以应用在map()转换操作中。

SourceFunction与SinkFunction

在DataStream API中，除了有MapFunction、FlatMapFunction等 转换函数之外，还有两种比较特殊的Function接口:SourceFunction 和SinkFunction。SourceFunction没有具体的数据元素输入，而是通 过在SourceFunction实现中与具体数据源建立连接，并读取指定数据 源中的数据，然后转换成StreamRecord数据结构发送到下游的 Operator中。SinkFunction接口的主要作用是将上游的数据元素输出 到外部数据源中。两种函数都具有比较独立的实现逻辑，下面我们分 别介绍SourceFunction和SinkFunction的设计和实现。

SourceFunction具体实现

```java
/**
 * Base interface for all stream data sources in Flink. The contract of a stream source is the
 * following: When the source should start emitting elements, the {@link #run} method is called with
 * a {@link SourceContext} that can be used for emitting elements. The run method can run for as
 * long as necessary. The source must, however, react to an invocation of {@link #cancel()} by
 * breaking out of its main loop.
 *
 * <h3>CheckpointedFunction Sources</h3>
 *
 * <p>Sources that also implement the {@link
 * org.apache.flink.streaming.api.checkpoint.CheckpointedFunction} interface must ensure that state
 * checkpointing, updating of internal state and emission of elements are not done concurrently.
 * This is achieved by using the provided checkpointing lock object to protect update of state and
 * emission of elements in a synchronized block.
 *
 * <p>This is the basic pattern one should follow when implementing a checkpointed source:
 *
 * <pre>{@code
 *  public class ExampleCountSource implements SourceFunction<Long>, CheckpointedFunction {
 *      private long count = 0L;
 *      private volatile boolean isRunning = true;
 *
 *      private transient ListState<Long> checkpointedCount;
 *
 *      public void run(SourceContext<T> ctx) {
 *          while (isRunning && count < 1000) {
 *              // this synchronized block ensures that state checkpointing,
 *              // internal state updates and emission of elements are an atomic operation
 *              synchronized (ctx.getCheckpointLock()) {
 *                  ctx.collect(count);
 *                  count++;
 *              }
 *          }
 *      }
 *
 *      public void cancel() {
 *          isRunning = false;
 *      }
 *
 *      public void initializeState(FunctionInitializationContext context) {
 *          this.checkpointedCount = context
 *              .getOperatorStateStore()
 *              .getListState(new ListStateDescriptor<>("count", Long.class));
 *
 *          if (context.isRestored()) {
 *              for (Long count : this.checkpointedCount.get()) {
 *                  this.count += count;
 *              }
 *          }
 *      }
 *
 *      public void snapshotState(FunctionSnapshotContext context) {
 *          this.checkpointedCount.clear();
 *          this.checkpointedCount.add(count);
 *      }
 * }
 * }</pre>
 *
 * <h3>Timestamps and watermarks:</h3>
 *
 * <p>Sources may assign timestamps to elements and may manually emit watermarks via the methods
 * {@link SourceContext#collectWithTimestamp(Object, long)} and {@link
 * SourceContext#emitWatermark(Watermark)}.
 *
 * @param <T> The type of the elements produced by this source.
 */
@Public
public interface SourceFunction<T> extends Function, Serializable {

    /**
     * Starts the source. Implementations use the {@link SourceContext} to emit elements. Sources
     * that checkpoint their state for fault tolerance should use the {@link
     * SourceContext#getCheckpointLock() checkpoint lock} to ensure consistency between the
     * bookkeeping and emitting the elements.
     *
     * <p>Sources that implement {@link CheckpointedFunction} must lock on the {@link
     * SourceContext#getCheckpointLock() checkpoint lock} checkpoint lock (using a synchronized
     * block) before updating internal state and emitting elements, to make both an atomic
     * operation.
     *
     * <p>Refer to the {@link SourceFunction top-level class docs} for an example.
     *
     * @param ctx The context to emit elements to and for accessing locks.
     */
    void run(SourceContext<T> ctx) throws Exception;

    /**
     * Cancels the source. Most sources will have a while loop inside the {@link
     * #run(SourceContext)} method. The implementation needs to ensure that the source will break
     * out of that loop after this method is called.
     *
     * <p>A typical pattern is to have an {@code "volatile boolean isRunning"} flag that is set to
     * {@code false} in this method. That flag is checked in the loop condition.
     *
     * <p>In case of an ungraceful shutdown (cancellation of the source operator, possibly for
     * failover), the thread that calls {@link #run(SourceContext)} will also be {@link
     * Thread#interrupt() interrupted}) by the Flink runtime, in order to speed up the cancellation
     * (to ensure threads exit blocking methods fast, like I/O, blocking queues, etc.). The
     * interruption happens strictly after this method has been called, so any interruption handler
     * can rely on the fact that this method has completed (for example to ignore exceptions that
     * happen after cancellation).
     *
     * <p>During graceful shutdown (for example stopping a job with a savepoint), the program must
     * cleanly exit the {@link #run(SourceContext)} method soon after this method was called. The
     * Flink runtime will NOT interrupt the source thread during graceful shutdown. Source
     * implementors must ensure that no thread interruption happens on any thread that emits records
     * through the {@code SourceContext} from the {@link #run(SourceContext)} method; otherwise the
     * clean shutdown may fail when threads are interrupted while processing the final records.
     *
     * <p>Because the {@code SourceFunction} cannot easily differentiate whether the shutdown should
     * be graceful or ungraceful, we recommend that implementors refrain from interrupting any
     * threads that interact with the {@code SourceContext} at all. You can rely on the Flink
     * runtime to interrupt the source thread in case of ungraceful cancellation. Any additionally
     * spawned threads that directly emit records through the {@code SourceContext} should use a
     * shutdown method that does not rely on thread interruption.
     */
    void cancel();

    // ------------------------------------------------------------------------
    //  source context
    // ------------------------------------------------------------------------

    /**
     * Interface that source functions use to emit elements, and possibly watermarks.
     *
     * @param <T> The type of the elements produced by the source.
     */
  //SourceContext主要用于收集SourceFunction中的 上下文信息
    @Public // Interface might be extended in the future with additional methods.
    interface SourceContext<T> {

        /**
         * Emits one element from the source, without attaching a timestamp. In most cases, this is
         * the default way of emitting elements.
         *
         * <p>The element will have no timestamp initially. If timestamps and watermarks are
         * required, for example for event-time windows, timers, or joins, then you need to assign a
         * timestamp via {@link DataStream#assignTimestampsAndWatermarks(WatermarkStrategy)} and set
         * a strategy that assigns timestamps (for example using {@link
         * WatermarkStrategy#withTimestampAssigner(TimestampAssignerSupplier)}).
         *
         * @param element The element to emit
         */
      //用于收集从外部数据源读取的数据并下发到下游 算子中。
        void collect(T element);

        /**
         * Emits one element from the source, and attaches the given timestamp.
         *
         * @param element The element to emit
         * @param timestamp The timestamp in milliseconds since the Epoch
         */
      //支持直接收集数据元素以及 EventTime时间戳。
        @PublicEvolving
        void collectWithTimestamp(T element, long timestamp);

        /**
         * Emits the given {@link Watermark}. A Watermark of value {@code t} declares that no
         * elements with a timestamp {@code t' <= t} will occur any more. If further such elements
         * will be emitted, those elements are considered <i>late</i>.
         *
         * @param mark The Watermark to emit
         */
      //用于在SourceFunction中生成Watermark并 发送到下游算子进行处理。
        @PublicEvolving
        void emitWatermark(Watermark mark);

        /**
         * Marks the source to be temporarily idle. This tells the system that this source will
         * temporarily stop emitting records and watermarks for an indefinite amount of time.
         *
         * <p>Source functions should make a best effort to call this method as soon as they
         * acknowledge themselves to be idle. The system will consider the source to resume activity
         * again once {@link SourceContext#collect(T)}, {@link SourceContext#collectWithTimestamp(T,
         * long)}, or {@link SourceContext#emitWatermark(Watermark)} is called to emit elements or
         * watermarks from the source.
         */
        @PublicEvolving
        void markAsTemporarilyIdle();

        /**
         * Returns the checkpoint lock. Please refer to the class-level comment in {@link
         * SourceFunction} for details about how to write a consistent checkpointed source.
         *
         * @return The object to use as the lock
         */
      //用于获取检查点锁(Checkpoint Lock)，例如使用KafkaConsumer读取数据时，可以使用检查点锁，确 保记录发出的原子性和偏移状态更新。
        Object getCheckpointLock();

        /** This method is called by the system to shut down the context. */
        void close();
    }
}
```

SourceFunction接口继承了Function接口，并在内 部定义了数据读取使用的run()方法和SourceContext内部类，其中 SourceContext定义了数据接入过程用到的上下文信息。在默认情况 下，SourceFunction不支持并行读取数据，因此SourceFunction被 ParallelSourceFunction接口继承，以支持对外部数据源中数据的并 行读取操作，比较典型的ParallelSourceFunction实例就是 FlinkKafkaConsumer。

```java
/**
 * A stream data source that is executed in parallel. Upon execution, the runtime will execute as
 * many parallel instances of this function as configured parallelism of the source.
 *
 * <p>This interface acts only as a marker to tell the system that this source may be executed in
 * parallel. When different parallel instances are required to perform different tasks, use the
 * {@link RichParallelSourceFunction} to get access to the runtime context, which reveals
 * information like the number of parallel tasks, and which parallel task the current instance is.
 *
 * @param <OUT> The type of the records produced by this source.
 */
@Public
public interface ParallelSourceFunction<OUT> extends SourceFunction<OUT> {}
```

RichParallelSourceFunction

```java
/**
 * Base class for implementing a parallel data source. Upon execution, the runtime will execute as
 * many parallel instances of this function as configured parallelism of the source.
 *
 * <p>The data source has access to context information (such as the number of parallel instances of
 * the source, and which parallel instance the current instance is) via {@link
 * #getRuntimeContext()}. It also provides additional life-cycle methods ({@link
 * #open(org.apache.flink.configuration.Configuration)} and {@link #close()}.
 *
 * @param <OUT> The type of the records produced by this source.
 */
@Public
public abstract class RichParallelSourceFunction<OUT> extends AbstractRichFunction
        implements ParallelSourceFunction<OUT> {

    private static final long serialVersionUID = 1L;
}
```

在SourceFunction的基础上拓展了 RichParallelSourceFunction和RichSourceFunction抽象实现类，这 使得SourceFunction可以在数据接入的过程中获取RuntimeContext信 息，从而实现更加复杂的操作，例如使用OperatorState保存Kafka中 数据消费的偏移量，从而实现端到端当且仅被处理一次的语义保障。

SourceContext主要有两种类型的实现子 类，分别为NonTimestampContext和WatermarkContext。顾名思义， WatermarkContext支持事件时间抽取和生成Watermark，最终用于处理 乱序事件;而NonTimestampContext不支持基于事件时间的操作，仅实 现了从外部数据源中读取数据并处理的逻辑，主要对应 TimeCharacteristic为ProcessingTime的情况。可以看出，用户设定 不同的TimeCharacteristic，就会创建不同类型的SourceContext，

其中AutomaticWatermarkContext和ManualWatermarkContext都继 承自WatermarkContext抽象类，分别对应接入时间和事件时间。由此 也可以看出，接入时间对应的Timestamp和Watermark都是通过Source 算子自动生成的。事件时间的实现则相对复杂，需要用户自定义 SourceContext.emitWatermark()方法来实现。

同时，SourceFunction接口的实现类主要通过run()方法完成与外 部数据源的交互，以实现外部数据的读取，并将读取到的数据通过 SourceContext提供的collect()方法发送给DataStream后续的算子进 行处理。常见的实现类有ContinuousFileMonitoringFunction、 FlinkKafkaConsumer等，这里我们以EventsGeneratorSource为例，简 单介绍SourceFunction接口的定义。

EventsGeneratorSource通过 SourceFunction.run()方法实现了事件的创建和采集，具体创建过程 主要通过EventsGenerator完成。实际上，在run()方法中会启动while 循环，不断调用EventsGenerator创建新的Event数据，最终通过 sourceContext.collect()方法对数据元素进行收集和下发，此时下游 算子可以接收到Event数据并进行处理。

EventsGeneratorSource.run()

```java
@Override
public void run(SourceContext<Event> sourceContext) throws Exception {
    final EventsGenerator generator = new EventsGenerator(errorProbability);

    final int range = Integer.MAX_VALUE / getRuntimeContext().getNumberOfParallelSubtasks();
    final int min = range * getRuntimeContext().getIndexOfThisSubtask();
    final int max = min + range;

    while (running) {
        sourceContext.collect(generator.next(min, max));

        if (delayPerRecordMillis > 0) {
            Thread.sleep(delayPerRecordMillis);
        }
    }
}
```

SourceFunction定义完毕后，会被封装在StreamSource算子中， 前面我们已经知道StreamSource继承自AbstractUdfStreamOperator。 在StreamSource算子中提供了run()方法实现SourceStreamTask实例的 调用和执行，SourceStreamTask实际上是针对Source类型算子实现的 StreamTask实现类。

SourceStreamTask.processInput

```java
@Override
protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {

    controller.suspendDefaultAction();

    // Against the usual contract of this method, this implementation is not step-wise but
    // blocking instead for
    // compatibility reasons with the current source interface (source functions run as a loop,
    // not in steps).
    sourceThread.setTaskDescription(getName());

    sourceThread.start();

    sourceThread
            .getCompletionFuture()
            .whenComplete(
                    (Void ignore, Throwable sourceThreadThrowable) -> {
                        if (sourceThreadThrowable != null) {
                            mailboxProcessor.reportThrowable(sourceThreadThrowable);
                        } else {
                            mailboxProcessor.suspend();
                        }
                    });
}
```

LegacySourceFunctionThread sourceThread.start();

```java
/** Runnable that executes the source function in the head operator. */
private class LegacySourceFunctionThread extends Thread {

    private final CompletableFuture<Void> completionFuture;

    LegacySourceFunctionThread() {
        this.completionFuture = new CompletableFuture<>();
    }

    @Override
    public void run() {
        try {
            if (!operatorChain.isTaskDeployedAsFinished()) {
                LOG.debug(
                        "Legacy source {} skip execution since the task is finished on restore",
                        getTaskNameWithSubtaskAndId());
                mainOperator.run(lock, operatorChain);
            }
            completeProcessing();
            completionFuture.complete(null);
        } catch (Throwable t) {
            // Note, t can be also an InterruptedException
            if (isCanceled()
                    && ExceptionUtils.findThrowable(t, InterruptedException.class)
                            .isPresent()) {
                completionFuture.completeExceptionally(new CancelTaskException(t));
            } else {
                completionFuture.completeExceptionally(t);
            }
        }
    }

    private void completeProcessing() throws InterruptedException, ExecutionException {
        if (!isCanceled() && !isFailing()) {
            mainMailboxExecutor
                    .submit(
                            () -> {
                                // theoretically the StreamSource can implement BoundedOneInput,
                                // so we need to call it here
                                final StopMode stopMode = finishingReason.toStopMode();
                                if (stopMode == StopMode.DRAIN) {
                                    operatorChain.endInput(1);
                                }
                                endData(stopMode);
                            },
                            "SourceStreamTask finished processing data.")
                    .get();
        }
    }

    public void setTaskDescription(final String taskDescription) {
        setName("Legacy Source Thread - " + taskDescription);
    }

    /**
     * @return future that is completed once this thread completes. If this task {@link
     *     #isFailing()} and this thread is not alive (e.g. not started) returns a normally
     *     completed future.
     */
    CompletableFuture<Void> getCompletionFuture() {
        return isFailing() && !isAlive()
                ? CompletableFuture.completedFuture(null)
                : completionFuture;
    }
}
```

mainOperator.run(lock, operatorChain);

```java
public void run(final Object lockingObject, final OperatorChain<?, ?> operatorChain)
        throws Exception {

    run(lockingObject, output, operatorChain);
}
```

run(lockingObject, output, operatorChain);

```java
public void run(
        final Object lockingObject,
        final Output<StreamRecord<OUT>> collector,
        final OperatorChain<?, ?> operatorChain)
        throws Exception {
//从OperatorConfig中获取TimeCharacteristic
    final TimeCharacteristic timeCharacteristic = getOperatorConfig().getTimeCharacteristic();
//从Task的环境信息 Environment中获取Configuration配置信息。
    final Configuration configuration =
            this.getContainingTask().getEnvironment().getTaskManagerInfo().getConfiguration();
    final long latencyTrackingInterval =
            getExecutionConfig().isLatencyTrackingConfigured()
                    ? getExecutionConfig().getLatencyTrackingInterval()
                    : configuration.getLong(MetricOptions.LATENCY_INTERVAL);
//创建LatencyMarksEmitter实例，主要用于在SourceFunction中输出 Latency标记，也就是周期性地生成时间戳，当下游算子接收到 SourceOperator发送的LatencyMark后，会使用当前的时间减去 LatencyMark中的时间戳，以此确认该算子数据处理的延迟情况，最后 算子会将LatencyMark监控指标以Metric的形式发送到外部的监控系统 中。
    LatencyMarkerEmitter<OUT> latencyEmitter = null;
    if (latencyTrackingInterval > 0) {
        latencyEmitter =
                new LatencyMarkerEmitter<>(
                        getProcessingTimeService(),
                        collector::emitLatencyMarker,
                        latencyTrackingInterval,
                        this.getOperatorID(),
                        getRuntimeContext().getIndexOfThisSubtask());
    }

    final long watermarkInterval =
            getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval();
//创建SourceContext，这里调用的是 StreamSourceContexts.getSourceContext()方法，在该方法中根据 TimeCharacteristic参数创建对应类型的SourceContext。
    this.ctx =
            StreamSourceContexts.getSourceContext(
                    timeCharacteristic,
                    getProcessingTimeService(),
                    lockingObject,
                    collector,
                    watermarkInterval,
                    -1,
                    emitProgressiveWatermarks);

    try {//调用userFunction.run(ctx)方法，调用和执行SourceFunction实例。
        userFunction.run(ctx);
    } finally {
        if (latencyEmitter != null) {
            latencyEmitter.close();
        }
    }
}
```

SinkFunction具体实现

相比于SourceFunction，SinkFunction的实现相对简单。在 SinkFunction中同样需要关注和外部介质的交互，尤其对于支持两阶 段提交的数据源来讲，此时需要使用TwoPhaseCommitSinkFunction实 现端到端的数据一致性。在SinkFunction中也会通过SinkContext获取 与Sink操作相关的上下文信息。

SinkFunction继承自Function接口，且 SinkFunciton分为WriteSink-Function和RichSinkFunction两种类型 的子类，其中WriteSinkFunction实现类已经被废弃，大部分情况下使 用的都是RichSinkFunction实现类。常见的RichSinkFunction实现类 有SocketClientSink和StreamingFileSink，对于支持两阶段提交的 TwoPhaseCommitSinkFunction，实现类主要有FlinkKafkaProducer。

和SourceFunction中的SourceContext一 样，在SinkFuntion中也会创建和使用SinkContext，以获取Sink操作 过程需要的上下文信息。但相比于SourceContext，SinkFuntion中的 SinkContext仅包含一些基本方法，例如获取 currentProcessingTime、currentWatermark以及Timestamp等变量。

在StreamSink Operator中提供了默认 SinkContext实现，通过SimpleContext可以从ProcessingTimeservice 中获取当前的处理时间、当前最大的Watermark和事件中的Timestamp 等信息。

SinkFunction.Context SinkFunction内部类

```java
/**
 * Context that {@link SinkFunction SinkFunctions } can use for getting additional data about an
 * input record.
 *
 * <p>The context is only valid for the duration of a {@link SinkFunction#invoke(Object,
 * Context)} call. Do not store the context and use afterwards!
 */
@Public // Interface might be extended in the future with additional methods.
interface Context {

    /** Returns the current processing time. */
    long currentProcessingTime();

    /** Returns the current event-time watermark. */
    long currentWatermark();

    /**
     * Returns the timestamp of the current input record or {@code null} if the element does not
     * have an assigned timestamp.
     */
    Long timestamp();
}
```

在StreamSink.processElement()方法中， 通过调用userFunction.invoke()方法触发Function计算，并将 sinkContext作为参数传递到userFunction中使用，此时SinkFunction 就能通过SinkContext提供的方法获取相应的时间信息并进行数据处 理，实现将数据发送至外部系统的功能。

StreamSink.processElement()

```java
@Override
public void processElement(StreamRecord<IN> element) throws Exception {
    sinkContext.element = element;
    userFunction.invoke(element.getValue(), sinkContext);
}
```

userFunction.invoke(element.getValue(), sinkContext); SinkFunction

```java
/**
 * Writes the given value to the sink. This function is called for every record.
 *
 * <p>You have to override this method when implementing a {@code SinkFunction}, this is a
 * {@code default} method for backward compatibility with the old-style method only.
 *
 * @param value The input record.
 * @param context Additional context about the input record.
 * @throws Exception This method may throw exceptions. Throwing an exception will cause the
 *     operation to fail and may trigger recovery.
 */
default void invoke(IN value, Context context) throws Exception {
    invoke(value);
}
```

TwoPhaseCommitSinkFunction主要用于需要严格保证数据当且仅 被输出一条的语义保障的场景。在TwoPhaseCommitSinkFunction中实 现了和外围数据交互过程的Transaction逻辑，也就是只有当数据真正 下发到外围存储介质时，才会认为Sink中的数据输出成功，其他任何 因素导致写入过程失败，都会对输出操作进行回退并重新发送数据。 目前所有Connector中支持TwoPhaseCommitSinkFunction的只有Kafka 消息中间件，且要求Kafka的版本在0.11以上。

ProcessFunction的定义与实现

在Flink API抽象栈中，最底层的是Stateful Function Process 接口，代码实现对应的是ProcessFunction接口。通过实现 ProcessFunction接口，能够灵活地获取底层处理数据和信息，例如状 态数据的操作、定时器的注册以及事件触发周期的控制等。

根据数据元素是否进行了KeyBy操作，可以将ProcessFunction分 为KeyedProcessFunction和ProcessFunction两种类型，其中 KeyedProcessFunction使用相对较多，常见的实现类有 TopNFunction、GroupAggFunction等函数;ProcessFunction的主要实 现类是LookupJoinRunner，主要用于实现维表的关联等操作。Table API模块相关的Operator直接实现自ProcessFunction接口。

ProcessFunction

```java
/**
 * A function that processes elements of a stream.
 *
 * <p>For every element in the input stream {@link #processElement(Object, Context, Collector)} is
 * invoked. This can produce zero or more elements as output. Implementations can also query the
 * time and set timers through the provided {@link Context}. For firing timers {@link #onTimer(long,
 * OnTimerContext, Collector)} will be invoked. This can again produce zero or more elements as
 * output and register further timers.
 *
 * <p><b>NOTE:</b> Access to keyed state and timers (which are also scoped to a key) is only
 * available if the {@code ProcessFunction} is applied on a {@code KeyedStream}.
 *
 * <p><b>NOTE:</b> A {@code ProcessFunction} is always a {@link
 * org.apache.flink.api.common.functions.RichFunction}. Therefore, access to the {@link
 * org.apache.flink.api.common.functions.RuntimeContext} is always available and setup and teardown
 * methods can be implemented. See {@link
 * org.apache.flink.api.common.functions.RichFunction#open(org.apache.flink.configuration.Configuration)}
 * and {@link org.apache.flink.api.common.functions.RichFunction#close()}.
 *
 * @param <I> Type of the input elements.
 * @param <O> Type of the output elements.
 */
@PublicEvolving
public abstract class ProcessFunction<I, O> extends AbstractRichFunction {

    private static final long serialVersionUID = 1L;

    /**
     * Process one element from the input stream.
     *
     * <p>This function can output zero or more elements using the {@link Collector} parameter and
     * also update internal state or set timers using the {@link Context} parameter.
     *
     * @param value The input value.
     * @param ctx A {@link Context} that allows querying the timestamp of the element and getting a
     *     {@link TimerService} for registering timers and querying the time. The context is only
     *     valid during the invocation of this method, do not store it.
     * @param out The collector for returning result values.
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *     operation to fail and may trigger recovery.
     */
    public abstract void processElement(I value, Context ctx, Collector<O> out) throws Exception;

    /**
     * Called when a timer set using {@link TimerService} fires.
     *
     * @param timestamp The timestamp of the firing timer.
     * @param ctx An {@link OnTimerContext} that allows querying the timestamp of the firing timer,
     *     querying the {@link TimeDomain} of the firing timer and getting a {@link TimerService}
     *     for registering timers and querying the time. The context is only valid during the
     *     invocation of this method, do not store it.
     * @param out The collector for returning result values.
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *     operation to fail and may trigger recovery.
     */
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<O> out) throws Exception {}

    /**
     * Information available in an invocation of {@link #processElement(Object, Context, Collector)}
     * or {@link #onTimer(long, OnTimerContext, Collector)}.
     */
    public abstract class Context {

        /**
         * Timestamp of the element currently being processed or timestamp of a firing timer.
         *
         * <p>This might be {@code null}, for example if the time characteristic of your program is
         * set to {@link org.apache.flink.streaming.api.TimeCharacteristic#ProcessingTime}.
         */
        public abstract Long timestamp();

        /** A {@link TimerService} for querying time and registering timers. */
        public abstract TimerService timerService();

        /**
         * Emits a record to the side output identified by the {@link OutputTag}.
         *
         * @param outputTag the {@code OutputTag} that identifies the side output to emit to.
         * @param value The record to emit.
         */
        public abstract <X> void output(OutputTag<X> outputTag, X value);
    }

    /**
     * Information available in an invocation of {@link #onTimer(long, OnTimerContext, Collector)}.
     */
    public abstract class OnTimerContext extends Context {
        /** The {@link TimeDomain} of the firing timer. */
        public abstract TimeDomain timeDomain();
    }
}
```

KeyedProcessFunction主要继承了 AbstractRichFunction抽象类，且在内部同时创建了Context和 OnTimerContext两个内部类，其中Context主要定义了从数据元素中获 取Timestamp和从运行时中获取TimerService等信息的方法，另外还有 用于旁路输出的output()方法。OnTimerContext则继承自Context抽象 类，主要应用在KeyedProcessFunction的OnTimer()方法中。在 KeyedProcessFunction中通过processElement方法读取数据元素并处 理，会在processElement()方法中根据实际情况创建定时器，此时定 时器会被注册到Context的TimerService定时器队列中，当满足定时器 触发的时间条件后，会通过调用OnTimer()方法执行定时器中的计算逻 辑，例如对状态数据的异步清理操作。

```java
/**
 * A keyed function that processes elements of a stream.
 *
 * <p>For every element in the input stream {@link #processElement(Object, Context, Collector)} is
 * invoked. This can produce zero or more elements as output. Implementations can also query the
 * time and set timers through the provided {@link Context}. For firing timers {@link #onTimer(long,
 * OnTimerContext, Collector)} will be invoked. This can again produce zero or more elements as
 * output and register further timers.
 *
 * <p><b>NOTE:</b> Access to keyed state and timers (which are also scoped to a key) is only
 * available if the {@code KeyedProcessFunction} is applied on a {@code KeyedStream}.
 *
 * <p><b>NOTE:</b> A {@code KeyedProcessFunction} is always a {@link
 * org.apache.flink.api.common.functions.RichFunction}. Therefore, access to the {@link
 * org.apache.flink.api.common.functions.RuntimeContext} is always available and setup and teardown
 * methods can be implemented. See {@link
 * org.apache.flink.api.common.functions.RichFunction#open(org.apache.flink.configuration.Configuration)}
 * and {@link org.apache.flink.api.common.functions.RichFunction#close()}.
 *
 * @param <K> Type of the key.
 * @param <I> Type of the input elements.
 * @param <O> Type of the output elements.
 */
@PublicEvolving
public abstract class KeyedProcessFunction<K, I, O> extends AbstractRichFunction {

    private static final long serialVersionUID = 1L;

    /**
     * Process one element from the input stream.
     *
     * <p>This function can output zero or more elements using the {@link Collector} parameter and
     * also update internal state or set timers using the {@link Context} parameter.
     *
     * @param value The input value.
     * @param ctx A {@link Context} that allows querying the timestamp of the element and getting a
     *     {@link TimerService} for registering timers and querying the time. The context is only
     *     valid during the invocation of this method, do not store it.
     * @param out The collector for returning result values.
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *     operation to fail and may trigger recovery.
     */
    public abstract void processElement(I value, Context ctx, Collector<O> out) throws Exception;

    /**
     * Called when a timer set using {@link TimerService} fires.
     *
     * @param timestamp The timestamp of the firing timer.
     * @param ctx An {@link OnTimerContext} that allows querying the timestamp, the {@link
     *     TimeDomain}, and the key of the firing timer and getting a {@link TimerService} for
     *     registering timers and querying the time. The context is only valid during the invocation
     *     of this method, do not store it.
     * @param out The collector for returning result values.
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *     operation to fail and may trigger recovery.
     */
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<O> out) throws Exception {}

    /**
     * Information available in an invocation of {@link #processElement(Object, Context, Collector)}
     * or {@link #onTimer(long, OnTimerContext, Collector)}.
     */
    public abstract class Context {

        /**
         * Timestamp of the element currently being processed or timestamp of a firing timer.
         *
         * <p>This might be {@code null}, for example if the time characteristic of your program is
         * set to {@link org.apache.flink.streaming.api.TimeCharacteristic#ProcessingTime}.
         */
        public abstract Long timestamp();

        /** A {@link TimerService} for querying time and registering timers. */
        public abstract TimerService timerService();

        /**
         * Emits a record to the side output identified by the {@link OutputTag}.
         *
         * @param outputTag the {@code OutputTag} that identifies the side output to emit to.
         * @param value The record to emit.
         */
        public abstract <X> void output(OutputTag<X> outputTag, X value);

        /** Get key of the element being processed. */
        public abstract K getCurrentKey();
    }

    /**
     * Information available in an invocation of {@link #onTimer(long, OnTimerContext, Collector)}.
     */
    public abstract class OnTimerContext extends Context {
        /** The {@link TimeDomain} of the firing timer. */
        public abstract TimeDomain timeDomain();

        /** Get key of the firing timer. */
        @Override
        public abstract K getCurrentKey();
    }
}
```

我们通过Table API中的ProcTimeRowsBoundedPrecedingFunction实例看 下KeyedProcessFunction的具体实现。 ProcTimeRowsBoundedPrecedingFunction主要用于对接入的数据去重，并保留最新的一行记录。

在 ProcTimeRowsBoundedPrecedingFunction.processElement()方法中定义了对 输入当前Function数据元素的处理逻辑。在方法中调用TimerService 获取当前的处理时间，然后基于该处理时间调用registerProcessingCleanupTimer()方法注册状态数据清理的定时 器，当处理时间到达注册时间后，就会调用定时器进行数据处理。

ProcTimeRowsBoundedPrecedingFunction

```java
@Override
public void processElement(
        RowData input,
        KeyedProcessFunction<K, RowData, RowData>.Context ctx,
        Collector<RowData> out)
        throws Exception {
    long currentTime = ctx.timerService().currentProcessingTime();
    // register state-cleanup timer
  // 注册状态数据清理的 Timer
    registerProcessingCleanupTimer(ctx, currentTime);

    // initialize state for the processed element
    RowData accumulators = accState.value();
    if (accumulators == null) {
        accumulators = function.createAccumulators();
    }
    // set accumulators in context first
    function.setAccumulators(accumulators);

    // get smallest timestamp
    Long smallestTs = smallestTsState.value();
    if (smallestTs == null) {
        smallestTs = currentTime;
        smallestTsState.update(smallestTs);
    }
    // get previous counter value
    Long counter = counterState.value();
    if (counter == null) {
        counter = 0L;
    }

    if (counter == precedingOffset) {
        List<RowData> retractList = inputState.get(smallestTs);
        if (retractList != null) {
            // get oldest element beyond buffer size
            // and if oldest element exist, retract value
            RowData retractRow = retractList.get(0);
            function.retract(retractRow);
            retractList.remove(0);
        } else {
            // Does not retract values which are outside of window if the state is cleared
            // already.
            LOG.warn(
                    "The state is cleared because of state ttl. "
                            + "This will result in incorrect result. "
                            + "You can increase the state ttl to avoid this.");
        }
        // if reference timestamp list not empty, keep the list
        if (retractList != null && !retractList.isEmpty()) {
            inputState.put(smallestTs, retractList);
        } // if smallest timestamp list is empty, remove and find new smallest
        else {
            inputState.remove(smallestTs);
            Iterator<Long> iter = inputState.keys().iterator();
            long currentTs = 0L;
            long newSmallestTs = Long.MAX_VALUE;
            while (iter.hasNext()) {
                currentTs = iter.next();
                if (currentTs < newSmallestTs) {
                    newSmallestTs = currentTs;
                }
            }
            smallestTsState.update(newSmallestTs);
        }
    } // we update the counter only while buffer is getting filled
    else {
        counter += 1;
        counterState.update(counter);
    }

    // update map state, counter and timestamp
    List<RowData> currentTimeState = inputState.get(currentTime);
    if (currentTimeState != null) {
        currentTimeState.add(input);
        inputState.put(currentTime, currentTimeState);
    } else { // add new input
        List<RowData> newList = new ArrayList<RowData>();
        newList.add(input);
        inputState.put(currentTime, newList);
    }

    // accumulate current row
    function.accumulate(input);
    // update the value of accumulators for future incremental computation
    accumulators = function.getAccumulators();
    accState.update(accumulators);

    // prepare output row
    RowData aggValue = function.getValue();
    output.replace(input, aggValue);
    out.collect(output);
}
```

registerProcessingCleanupTimer() KeyedProcessFunctionWithCleanupState

```java
protected void registerProcessingCleanupTimer(Context ctx, long currentTime) throws Exception {
    if (stateCleaningEnabled) {
        registerProcessingCleanupTimer(
                cleanupTimeState,
                currentTime,
                minRetentionTime,
                maxRetentionTime,
                ctx.timerService());
    }
}
```

CleanupState.registerProcessingCleanupTimer

```java
/**
 * Base interface for clean up state, both for {@link ProcessFunction} and {@link
 * CoProcessFunction}.
 */
public interface CleanupState {

    default void registerProcessingCleanupTimer(
            ValueState<Long> cleanupTimeState,
            long currentTime,
            long minRetentionTime,
            long maxRetentionTime,
            TimerService timerService)
            throws Exception {

        // last registered timer
      //通过cleanupTimeState状态获取最新一次清理状态的注册时间 curCleanupTime。
        Long curCleanupTime = cleanupTimeState.value();

        // check if a cleanup timer is registered and
        // that the current cleanup timer won't delete state we need to keep
      //判断当前curCleanupTime是否为空，且 currentTime+minRetentionTime总和是否大于curCleanupTime。只有满足 以上两个条件才会触发注册状态数据清理的定时器，这里的 minRetentionTime是用户指定的状态保留最短时间。
        if (curCleanupTime == null || (currentTime + minRetentionTime) > curCleanupTime) {
            // we need to register a new (later) timer
            long cleanupTime = currentTime + maxRetentionTime;
            // register timer and remember clean-up time
          //如果以上条件都满足，则调用TimerService注册 ProcessingTimeTimer，在满足定时器的时间条件后触发定时器。
            timerService.registerProcessingTimeTimer(cleanupTime);
            // delete expired timer
          //如果curCleanupTime不为空，即之前的TimerService还包含过期的 定时器，则调用timerService.deleteProcessingTimeTimer()方法删除过期的定时器。
            if (curCleanupTime != null) {
                timerService.deleteProcessingTimeTimer(curCleanupTime);
            }
          //更新cleanupTimeState中的curCleanupTime指标。
            cleanupTimeState.update(cleanupTime);
        }
    }
}
```

触发注册的Timer后，调用 ProcTimeRowsBoundedPrecedingFunction.onTimer()方法处理数据。

在onTimer()方法中，主要调用 cleanupState()方法对状态进行清理。逻辑相对简单，实际上就是 ProcessFunction的定义和实现。当然除了对状态数据的清理，也可以 通过定时器完成其他类型的定时操作，实现更加复杂的计算逻辑，定 时器最终都会注册在TimerService内部的队列中。

```java
@Override
public void onTimer(
        long timestamp,
        KeyedProcessFunction<K, RowData, RowData>.OnTimerContext ctx,
        Collector<RowData> out)
        throws Exception {
    if (stateCleaningEnabled) {
        cleanupState(inputState, accState, counterState, smallestTsState);
        function.cleanup();
    }
}
```

除了上面介绍的，还有其他类型的ProcessFunction实现，但不管 是哪种类型的实现，基本都是这些功能的组合或变体。在DataStream API中实际上基于ProcessFunction定义了很多可以直接使用的方法。 虽然ProcessFunction接口更加灵活，但使用复杂度也相对较高，因此 除非无法通过现成算子实现复杂的计算逻辑，通常情况下用户是不需 要自定义实现ProcessFunction处理数据的。