在整个流数据处理的过程中，针对时间信息的处理可以说是非常 普遍的，尤其在涉及窗口计算时，会根据设定的TimeCharacteristic 是事件时间还是处理时间，选择不同的数据方式处理接入的数据。

那么在Operator中如何对时间信息进行有效的协调和管理呢?在 每个Operator内部都维系了一个TimerService，专门用于处理与时间 相关的操作。例如获取当前算子中最新的处理时间以及Watermark、注 册不同时间类型的定时器等。我们已经知道，在ProcessFunction中会 非常频繁地使用TimerService定义和使用定时器，以完成复杂的数据 转换操作。接下来我们重点了解TimerService组件的设计和实现。

时间概念与Watermark

在Flink中，时间概念主要分为三种类型，即事件时间、处理时间 以及接入时间

通过如下三种方式可以抽获和生成Timestamp和Watermark。

1.在SourceFunction中抽取Timestamp和生成Watermark

在SourceFunction中读取数据元素时，SourceContext接口中定义 了抽取Timestamp和生成Watermark的方法，如 collectWithTimestamp(T element, long timestamp)和 emitWatermark(Watermark mark)方法。如果Flink作业基于事件时间 的概念，就会使用StreamSourceContexts.ManualWatermarkContext处 理Watermark信息。

WatermarkContext.collectWithTimestamp 方法直接从Source算子接入的数据中抽取事件时间的时间戳信息。

```java
@Override
public final void collectWithTimestamp(T element, long timestamp) {
    synchronized (checkpointLock) {
        processAndEmitWatermarkStatus(WatermarkStatus.ACTIVE);

        if (nextCheck != null) {
            this.failOnNextCheck = false;
        } else {
            scheduleNextIdleDetectionTask();
        }
//抽取Timestamp信息
        processAndCollectWithTimestamp(element, timestamp);
    }
}
```

生成Watermark主要是通过调用 WatermarkContext.emitWatermark()方法进行的。生成的Watermark首 先会更新当前Source算子中的CurrentWatermark，然后将Watermark传 递给下游的算子继续处理。当下游算子接收到Watermark事件后，也会 更新当前算子内部的CurrentWatermark。

SourceFunction接口主要调用 WatermarkContext.emitWatermark()方法生成并输出Watermark事件， 在emitWatermark()方法中会调用processAndEmitWatermark()方法将 生成的Watermark实时发送到下游算子中继续处理。

```java
@Override
public final void emitWatermark(Watermark mark) {
    if (allowWatermark(mark)) {
        synchronized (checkpointLock) {
            processAndEmitWatermarkStatus(WatermarkStatus.ACTIVE);

            if (nextCheck != null) {
                this.failOnNextCheck = false;
            } else {
                scheduleNextIdleDetectionTask();
            }

            processAndEmitWatermark(mark);
        }
    }
}
```

2.通过DataStream中的独立算子抽取Timestamp和生成Watermark

除了能够在SourceFunction中直接分配Timestamp和生成 Watermark，也可以在DataStream数据转换的过程中进行相应操作，此 时转换操作对应的算子就能使用生成的Timestamp和Watermark信息 了。

在DataStream API中提供了3种与抽取Timestamp和生成Watermark 相关的Function接口，分别为TimestampExtractor、 AssignerWithPeriodicWatermarks以及 AssignerWithPunctuatedWatermarks。

在TimestampAssigner接口中定义抽取Timestamp 的方法。然后分别在AssignerWithPeriodicWatermarks和 AssignerWithPunctuatedWatermarks接口中定义生成Watermark的方 法。在早期的TimestampExtractor实现中同时包含了Timestamp抽取与 生成Watermark的逻辑。

·AssignerWithPeriodicWatermarks:事件时间驱动，会周期性地根 据事件时间与当前算子中最大的Watermark进行对比，如果当前的 EventTime大于Watermark，则触发Watermark更新逻辑，将最新的 EventTime赋予CurrentWatermark，并将新生成的Watermark推送至下游 算子。

·AssignerWithPunctuatedWatermarks:特殊事件驱动，主要根据数 据元素中的特殊事件生成Watermark。例如数据中有产生Watermark的标记，接入数据元素时就会根据该标记调用相关方法生成Watermark。

需要注意的是，AssignerWithPeriodicWatermarks中生成 Watermark的默认周期为0，用户可以根据具体情况对周期进行调整， 但周期过大会增加数据处理的时延。

如果接入事件中的Timestamp是单调 递增的，即不会出现乱序的情况，就可以直接使用 AssignerWithPeriodicWatermarks接口的默认抽象实现类 AscendingTimestampExtractor自动生成Watermark。另外，对于接入 数据是有界乱序的情况，可以使用 BoundedOutOfOrdernessTimestampExtractor实现类生成Watermark事 件。但不论是AscendingTimestampExtractor还是 BoundedOutOfOrdernessTimestampExtractor实现类，都需要用户实现 extractTimestamp()方法获取EventTime信息。

当用户通过实现 AssignerWithPeriodicWatermarks抽象类，并调用 DataStream.assignTimestampsAndWatermarks()方法时，实际上会根 据传入的AssignerWithPeriodicWatermarks创建 TimestampsAndPeriodicWatermarksOperator算子。最后调用 DataStream.transform()方法将该Operator封装在Transformation 中。因此这种获取EventTime和Watermark的方式是通过单独定义算子 实现的。

DataStream.assignTimestampsAndWatermarks

```java
/**
 * Assigns timestamps to the elements in the data stream and generates watermarks to signal
 * event time progress. The given {@link WatermarkStrategy} is used to create a {@link
 * TimestampAssigner} and {@link WatermarkGenerator}.
 *
 * <p>For each event in the data stream, the {@link TimestampAssigner#extractTimestamp(Object,
 * long)} method is called to assign an event timestamp.
 *
 * <p>For each event in the data stream, the {@link WatermarkGenerator#onEvent(Object, long,
 * WatermarkOutput)} will be called.
 *
 * <p>Periodically (defined by the {@link ExecutionConfig#getAutoWatermarkInterval()}), the
 * {@link WatermarkGenerator#onPeriodicEmit(WatermarkOutput)} method will be called.
 *
 * <p>Common watermark generation patterns can be found as static methods in the {@link
 * org.apache.flink.api.common.eventtime.WatermarkStrategy} class.
 *
 * @param watermarkStrategy The strategy to generate watermarks based on event timestamps.
 * @return The stream after the transformation, with assigned timestamps and watermarks.
 */
public SingleOutputStreamOperator<T> assignTimestampsAndWatermarks(
        WatermarkStrategy<T> watermarkStrategy) {
    final WatermarkStrategy<T> cleanedStrategy = clean(watermarkStrategy);
    // match parallelism to input, to have a 1:1 source -> timestamps/watermarks relationship
    // and chain
    final int inputParallelism = getTransformation().getParallelism();
  // 生成TimestampsAndWatermarksTransformation
    final TimestampsAndWatermarksTransformation<T> transformation =
            new TimestampsAndWatermarksTransformation<>(
                    "Timestamps/Watermarks",
                    inputParallelism,
                    getTransformation(),
                    cleanedStrategy,
                    false);
  //将生成的Operator加入Transformation列表
    getExecutionEnvironment().addOperator(transformation);
    return new SingleOutputStreamOperator<>(getExecutionEnvironment(), transformation);
}
```

3.通过Connector提供的接口抽取Timestamp和生成Watermark

对于某些内置的数据源连接器来讲，是通过实现SourceFunction 接口接入外部数据的，此时用户无法直接获取SourceFunction的接口 方法，会造成无法在SourceOperator中直接生成EventTime和 Watermark的情况。在FlinkKafkaConsumer和FlinkKinesisConsumer这 些内置的数据源连接器中，已经支持用户将 AssignerWithPeriodicWatermarks和 AssignerWithPunctuatedWatermarks实现类传递到连接器的接口中， 然后再通过连接器应用在对应的SourceFunction中，进而生成 EventTime和Watermark。

FlinkKafkaConsumer提供了 FlinkKafkaConsumerBase.assignTimestampsAndWatermarks()方法， 用于设定创建AssignerWithPeriodicWatermarks或 AssignerWithPunctuatedWatermarks实现类。

```java
/**
 * Sets the given {@link WatermarkStrategy} on this consumer. These will be used to assign
 * timestamps to records and generates watermarks to signal event time progress.
 *
 * <p>Running timestamp extractors / watermark generators directly inside the Kafka source
 * (which you can do by using this method), per Kafka partition, allows users to let them
 * exploit the per-partition characteristics.
 *
 * <p>When a subtask of a FlinkKafkaConsumer source reads multiple Kafka partitions, the streams
 * from the partitions are unioned in a "first come first serve" fashion. Per-partition
 * characteristics are usually lost that way. For example, if the timestamps are strictly
 * ascending per Kafka partition, they will not be strictly ascending in the resulting Flink
 * DataStream, if the parallel source subtask reads more than one partition.
 *
 * <p>Common watermark generation patterns can be found as static methods in the {@link
 * org.apache.flink.api.common.eventtime.WatermarkStrategy} class.
 *
 * @return The consumer object, to allow function chaining.
 */
public FlinkKafkaConsumerBase<T> assignTimestampsAndWatermarks(
        WatermarkStrategy<T> watermarkStrategy) {
    checkNotNull(watermarkStrategy);

    try {
        ClosureCleaner.clean(
                watermarkStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
        this.watermarkStrategy = new SerializedValue<>(watermarkStrategy);
    } catch (Exception e) {
        throw new IllegalArgumentException(
                "The given WatermarkStrategy is not serializable", e);
    }

    return this;
}
```

TimerService时间服务

对于需要依赖时间定时器进行数据处理的算子来讲，需要借助 TimerService组件实现对定时器的管理，其中定时器执行的具体处理 逻辑主要通过回调函数定义。每个StreamOperator在创建和初始化的 过程中，都会通过InternalTimeServiceManager创建TimerService实 例，这里的InternalTimeServiceManager管理了Task内所有和时间相 关的服务，并向所有Operator提供创建和获取TimerService的方法。

1.TimerService的设计与实现

我们先来看下TimerService的设计与实现，在 DataStream API中提供了TimerService接口，用于获取和操作时间相 关的信息。TimerService接口的默认实现有SimpleTimerService，在 Flink Table API模块的 AbstractProcessStreamOperator.ContextImpl内部类中也实现了 TimerService接口。从图中可以看出，SimpleTimerService会将 InternalTimerService接口作为内部成员变量，因此在 SimpleTimerService中提供的方法基本上都是借助 InternalTimerService实现的。

InternalTimerService实际上是TimerService接口的内部版本， 而TimerService接口是专门供用户使用的外部接口。 InternalTimerService需要按照Key和命名空间进行划分，并提供操作 时间和定时器的内部方法，因此不仅是SimpleTimerService通过 InternalTimerService操作和获取时间信息以及定时器，其他还有如 WindowOperator、IntervalJoinOperator等内置算子也都会通过 InternalTimerService提供的方法执行时间相关的操作。

TimerService应用举例

接下来我们以KeyedProcessFunction实现类 ProcTimeDeduplicateKeepLastRowFunction为例，详细说明在自定义函数中如 何通过调用和操作TimerService服务实现时间信息的获取和定时器的 注册。

KeyedProcessOperator.open()

```java
@Override
public void open() throws Exception {
    super.open();
    collector = new TimestampedCollector<>(output);
//调用getInternalTimerService()方法创建和获取InternalTimerService 实例。实际上最终调用的是 AbstractStreamOperator.getInternalTimerService()方法获取 InternalTimervService实例。
    InternalTimerService<VoidNamespace> internalTimerService =
            getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);
//基于InternalTimerService实例创建SimpleTimerService实例。
    TimerService timerService = new SimpleTimerService(internalTimerService);
//将创建好的SimpleTimerService封装在ContextImpl和 OnTimerContextImpl上下文对象中，此时KeyedProcessFunction的实现类 就可以通过上下文获取SimpleTimerService实例了。
    context = new ContextImpl(userFunction, timerService);
    onTimerContext = new OnTimerContextImpl(userFunction, timerService);
}
```

ProcTimeRangeBoundedPrecedingFunction

```
public class ProcTimeRangeBoundedPrecedingFunction<K>
        extends KeyedProcessFunction<K, RowData, RowData> {
```

ProcTimeRangeBoundedPrecedingFunction.processElement

```java
@Override
public void processElement(
        RowData input,
        KeyedProcessFunction<K, RowData, RowData>.Context ctx,
        Collector<RowData> out)
        throws Exception {
    long currentTime = ctx.timerService().currentProcessingTime();
    // buffer the event incoming event

    // add current element to the window list of elements with corresponding timestamp
    List<RowData> rowList = inputState.get(currentTime);
    // null value means that this is the first event received for this timestamp
    if (rowList == null) {
        rowList = new ArrayList<RowData>();
        // register timer to process event once the current millisecond passed
        ctx.timerService().registerProcessingTimeTimer(currentTime + 1);
        registerCleanupTimer(ctx, currentTime);
    }
    rowList.add(input);
    inputState.put(currentTime, rowList);
}
```

registerCleanupTimer(ctx, currentTime);

```java
private void registerCleanupTimer(
        KeyedProcessFunction<K, RowData, RowData>.Context ctx, long timestamp)
        throws Exception {
    // calculate safe timestamp to cleanup states
    long minCleanupTimestamp = timestamp + precedingTimeBoundary + 1;
    long maxCleanupTimestamp = timestamp + (long) (precedingTimeBoundary * 1.5) + 1;
    // update timestamp and register timer if needed
    Long curCleanupTimestamp = cleanupTsState.value();
    if (curCleanupTimestamp == null || curCleanupTimestamp < minCleanupTimestamp) {
        // we don't delete existing timer since it may delete timer for data processing
        // TODO Use timer with namespace to distinguish timers
        ctx.timerService().registerProcessingTimeTimer(maxCleanupTimestamp);
        cleanupTsState.update(maxCleanupTimestamp);
    }
}
```

对于registerProcessingCleanupTimer()方法，实际上就是调用 timerService.registerProcessingTimeTimer(cleanupTime)注册基于 处理时间的定时器。

系统时间到达Timer指定的时间后，TimerService会调用和触发注 册的定时器，然后调用ProcTimeRangeBoundedPrecedingFunction.onTimer() 方法。从onTimer()方法定义中可以看出，调用cleanupState()方法完成了对指定状态数据的清理操作

```java
@Override
public void onTimer(
        long timestamp,
        KeyedProcessFunction<K, RowData, RowData>.OnTimerContext ctx,
        Collector<RowData> out)
        throws Exception {
    Long cleanupTimestamp = cleanupTsState.value();
    // if cleanupTsState has not been updated then it is safe to cleanup states
    if (cleanupTimestamp != null && cleanupTimestamp <= timestamp) {
        inputState.clear();
        accState.clear();
        cleanupTsState.clear();
        function.cleanup();
        return;
    }

    // remove timestamp set outside of ProcessFunction.
    ((TimestampedCollector) out).eraseTimestamp();

    // we consider the original timestamp of events
    // that have registered this time trigger 1 ms ago

    long currentTime = timestamp - 1;

    // get the list of elements of current proctime
    List<RowData> currentElements = inputState.get(currentTime);

    // Expired clean-up timers pass the needToCleanupState check.
    // Perform a null check to verify that we have data to process.
    if (null == currentElements) {
        return;
    }

    // initialize the accumulators
    RowData accumulators = accState.value();

    if (null == accumulators) {
        accumulators = function.createAccumulators();
    }

    // set accumulators in context first
    function.setAccumulators(accumulators);

    // update the elements to be removed and retract them from aggregators
    long limit = currentTime - precedingTimeBoundary;

    // we iterate through all elements in the window buffer based on timestamp keys
    // when we find timestamps that are out of interest, we retrieve corresponding elements
    // and eliminate them. Multiple elements could have been received at the same timestamp
    // the removal of old elements happens only once per proctime as onTimer is called only once
    Iterator<Long> iter = inputState.keys().iterator();
    List<Long> markToRemove = new ArrayList<Long>();
    while (iter.hasNext()) {
        Long elementKey = iter.next();
        if (elementKey < limit) {
            // element key outside of window. Retract values
            List<RowData> elementsRemove = inputState.get(elementKey);
            if (elementsRemove != null) {
                int iRemove = 0;
                while (iRemove < elementsRemove.size()) {
                    RowData retractRow = elementsRemove.get(iRemove);
                    function.retract(retractRow);
                    iRemove += 1;
                }
            } else {
                // Does not retract values which are outside of window if the state is cleared
                // already.
                LOG.warn(
                        "The state is cleared because of state ttl. "
                                + "This will result in incorrect result. "
                                + "You can increase the state ttl to avoid this.");
            }

            // mark element for later removal not to modify the iterator over MapState
            markToRemove.add(elementKey);
        }
    }

    // need to remove in 2 steps not to have concurrent access errors via iterator to the
    // MapState
    int i = 0;
    while (i < markToRemove.size()) {
        inputState.remove(markToRemove.get(i));
        i += 1;
    }

    // add current elements to aggregator. Multiple elements might
    // have arrived in the same proctime
    // the same accumulator value will be computed for all elements
    int iElemenets = 0;
    while (iElemenets < currentElements.size()) {
        RowData input = currentElements.get(iElemenets);
        function.accumulate(input);
        iElemenets += 1;
    }

    // we need to build the output and emit for every event received at this proctime
    iElemenets = 0;
    RowData aggValue = function.getValue();
    while (iElemenets < currentElements.size()) {
        RowData input = currentElements.get(iElemenets);
        output.replace(input, aggValue);
        out.collect(output);
        iElemenets += 1;
    }

    // update the value of accumulators for future incremental computation
    accumulators = function.getAccumulators();
    accState.update(accumulators);
}
```

InternalTimerService详解

```java
/**
 * Interface for working with time and timers.
 *
 * <p>This is the internal version of {@link org.apache.flink.streaming.api.TimerService} that
 * allows to specify a key and a namespace to which timers should be scoped.
 *
 * @param <N> Type of the namespace to which timers are scoped.
 */
@Internal
public interface InternalTimerService<N> {

    /** Returns the current processing time. */
  //获取当前的处理时间。
    long currentProcessingTime();

    /** Returns the current event-time watermark. */
  //获取当前算子基于事件时间的Watermark。
    long currentWatermark();

    /**
     * Registers a timer to be fired when processing time passes the given time. The namespace you
     * pass here will be provided when the timer fires.
     */
  //注册基于处理时间的定时器。
    void registerProcessingTimeTimer(N namespace, long time);

    /** Deletes the timer for the given key and namespace. */
  //删除基于处理时间的定时器。
    void deleteProcessingTimeTimer(N namespace, long time);

    /**
     * Registers a timer to be fired when event time watermark passes the given time. The namespace
     * you pass here will be provided when the timer fires.
     */
  //注册基于事件时间的定时器
    void registerEventTimeTimer(N namespace, long time);

    /** Deletes the timer for the given key and namespace. */
  //删除基于事件时间的定时器。
    void deleteEventTimeTimer(N namespace, long time);

    /**
     * Performs an action for each registered timer. The timer service will set the key context for
     * the timers key before invoking the action.
     */
    void forEachEventTimeTimer(BiConsumerWithException<N, Long, Exception> consumer)
            throws Exception;

    /**
     * Performs an action for each registered timer. The timer service will set the key context for
     * the timers key before invoking the action.
     */
    void forEachProcessingTimeTimer(BiConsumerWithException<N, Long, Exception> consumer)
            throws Exception;
}
```

InternalTimerService接口具有InternalTimerServiceImpl的默 认实现类，在InternalTimerServiceImpl中，实际上包含了两个比较 重要的成员变量，分别为processingTimeService和 KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> 队列。其中processingTimeService是基于系统处理时间提供的 TimerService，也就是说，基于ProcessingTimeService的实现类可以 注册基于处理时间的定时器。TimerHeapInternalTimer队列主要分为 processingTimeTimersQueue和eventTimeTimersQueue两种类型，用于 存储相应类型的定时器队列。TimerHeapInternalTimer基于Heap堆内 存存储定时器，并通过HeapPriorityQueueSet结构存储注册好的定时 器。

在InternalTimerServiceImpl中，会记录currentWatermark信 息，用于表示当前算子的最新Watermark，实际上 InternalTimerServiceImpl实现了基于Watermark的时钟，此时算子会 递增更新InternalTimerServiceImpl中Watermark对应的时间戳。此时 InternalTimerService会判断eventTimeTimersQueue队列中是否有定 时器、是否满足触发条件，如果满足则将相应的 TimerHeapInternalTimer取出，并执行对应算子中的onEventTime()回 调方法，此时就和ProcessFunction中的onTimer()方法联系在一起 了。

以IntervalJoinOperator为例说明内部算子如何直接调 用InternalTimerService注册定时器。

在 IntervalJoinOperator.processElement()方法中，实际上会调用 internalTimerService.registerEventTimeTimer()方法注册基于事件 时间的定时器，专门用于数据清理任务。随后internalTimerService 会根据指定的cleanupTime完成对窗口中历史状态数据的清理。

```java
private <THIS, OTHER> void processElement(
        final StreamRecord<THIS> record,
        final MapState<Long, List<IntervalJoinOperator.BufferEntry<THIS>>> ourBuffer,
        final MapState<Long, List<IntervalJoinOperator.BufferEntry<OTHER>>> otherBuffer,
        final long relativeLowerBound,
        final long relativeUpperBound,
        final boolean isLeft)
        throws Exception {

    final THIS ourValue = record.getValue();
    final long ourTimestamp = record.getTimestamp();

    if (ourTimestamp == Long.MIN_VALUE) {
        throw new FlinkException(
                "Long.MIN_VALUE timestamp: Elements used in "
                        + "interval stream joins need to have timestamps meaningful timestamps.");
    }

    if (isLate(ourTimestamp)) {
        sideOutput(ourValue, ourTimestamp, isLeft);
        return;
    }

    addToBuffer(ourBuffer, ourValue, ourTimestamp);

    for (Map.Entry<Long, List<BufferEntry<OTHER>>> bucket : otherBuffer.entries()) {
        final long timestamp = bucket.getKey();

        if (timestamp < ourTimestamp + relativeLowerBound
                || timestamp > ourTimestamp + relativeUpperBound) {
            continue;
        }

        for (BufferEntry<OTHER> entry : bucket.getValue()) {
            if (isLeft) {
                collect((T1) ourValue, (T2) entry.element, ourTimestamp, timestamp);
            } else {
                collect((T1) entry.element, (T2) ourValue, timestamp, ourTimestamp);
            }
        }
    }

    long cleanupTime =
            (relativeUpperBound > 0L) ? ourTimestamp + relativeUpperBound : ourTimestamp;
    if (isLeft) {
        internalTimerService.registerEventTimeTimer(CLEANUP_NAMESPACE_LEFT, cleanupTime);
    } else {
        internalTimerService.registerEventTimeTimer(CLEANUP_NAMESPACE_RIGHT, cleanupTime);
    }
}
```

当StreamOperator算子中的Watermark更新 时，就会通过InternalTimeServiceManager通知所有的 InternalTimerService实例，这里实际上就是调用 InternalTimerServiceImpl.advanceWatermark()方法实现的。从 advanceWatermark()方法中可以看出，首先会通过最新的时间更新 currentWatermark，然后从eventTimeTimersQueue队列中获取事件时 间定时器，最后判断timer.getTimestamp()是否小于接入的time变 量，如果小于，则说明当前算子的时间大于定时器中设定的时间，此 时就会执行triggerTarget.onEventTime (timer)方法，这里的 triggerTarget实际上就是StreamOperator的具体实现类。