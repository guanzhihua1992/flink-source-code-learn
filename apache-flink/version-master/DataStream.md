DataStream代表一系列同类型数据的集合，可以通过转换操作生 成新的DataStream。DataStream用于表达业务转换逻辑，实际上并没 有存储真实数据。

```java
@Public
public class DataStream<T> {

    protected final StreamExecutionEnvironment environment;
//当前DataStream对应的上一 次的转换操作，换句话讲，就是通过transformation生成当前的 DataStream。
    protected final Transformation<T> transformation;
}
```

DataStream之间的转换操作都是通过 Transformation结构展示的，例如当用户执行 DataStream.map()方法转换时，底层对应的便是 OneInputTransformation转换操作。

```java
/**
 * Applies a Map transformation on a {@link DataStream}. The transformation calls a {@link
 * MapFunction} for each element of the DataStream. Each MapFunction call returns exactly one
 * element. The user can also extend {@link RichMapFunction} to gain access to other features
 * provided by the {@link org.apache.flink.api.common.functions.RichFunction} interface.
 
 *MapFunction 对应的操作是进一出一 可以自定义实现 RichMapFunction
 */
public <R> SingleOutputStreamOperator<R> map(MapFunction<T, R> mapper) {

    TypeInformation<R> outType =
            TypeExtractor.getMapReturnTypes(
                    clean(mapper), getType(), Utils.getCallLocationName(), true);

    return map(mapper, outType);
}
```

map(mapper, outType);

```java
public <R> SingleOutputStreamOperator<R> map(
        MapFunction<T, R> mapper, TypeInformation<R> outputType) {
  //传入的三个参数。"Map" outputType 返回值类型TypeInformation  StreamMap
    return transform("Map", outputType, new StreamMap<>(clean(mapper)));
}
```

transform("Map", outputType, new StreamMap<>(clean(mapper)))

```java
@PublicEvolving
public <R> SingleOutputStreamOperator<R> transform(
        String operatorName,
        TypeInformation<R> outTypeInfo,
        OneInputStreamOperator<T, R> operator) {

    return doTransform(operatorName, outTypeInfo, SimpleOperatorFactory.of(operator));
}
```

doTransform(operatorName, outTypeInfo, SimpleOperatorFactory.of(operator))

```java
protected <R> SingleOutputStreamOperator<R> doTransform(
        String operatorName,
        TypeInformation<R> outTypeInfo,
        StreamOperatorFactory<R> operatorFactory) {

    // read the output type of the input Transform to coax out errors about MissingTypeInfo
  //从上一次转换操作中获取TypeInformation信息，确定没有出现 MissingTypeInfo错误，以确保下游算子转换不会出现问题
    transformation.getOutputType();
//基于operatorName、outTypeInfo和operatorFactory等参数创建 OneInputTransformation实例，注意OneInputTransformation也会包含当 前DataStream对应的上一次转换操作。
    OneInputTransformation<T, R> resultTransform =
            new OneInputTransformation<>(
                    this.transformation,
                    operatorName,
                    operatorFactory,
                    outTypeInfo,
                    environment.getParallelism(),
                    false);
//基于OneInputTransformation实例创建 SingleOutputStreamOperator。SingleOutputStreamOperator继承了 DataStream类，属于特殊的DataStream，主要用于每次转换操作后返回 给用户继续操作的数据结构。SingleOutputStreamOperator额外提供了 returns()、disableChaining()等方法供用户使用。
    @SuppressWarnings({"unchecked", "rawtypes"})
    SingleOutputStreamOperator<R> returnStream =
            new SingleOutputStreamOperator(environment, resultTransform);
//调用getExecutionEnvironment().addOperator(resultTransform)方 法，将创建好的OneInputTransformation添加到 StreamExecutionEnvironment的Transformation集合中，用于生成 StreamGraph对象。
    getExecutionEnvironment().addOperator(resultTransform);
//将returnStream返回给用户，继续执行后续的转换操作。基于这 样连续的转换操作，将所有DataStream之间的转换按顺序存储在StreamExecutionEnvironment中。
    return returnStream;
}
```

在DataStream转换的过程中，不管是哪种类型的转换操作，都是 按照同样的方式进行的:首先将用户自定义的函数封装到Operator 中，然后将Operator封装到Transformation转换操作结构中，最后将 Transformation写入StreamExecutionEnvironment提供的 Transformation集合。通过DataStream之间的转换操作形成Pipeline 拓扑，即StreamGraph数据结构，最终通过StreamGraph生成JobGraph 并提交到集群上运行。

new OneInputTransformation

```java
public OneInputTransformation(
        Transformation<IN> input,
        String name,
        StreamOperatorFactory<OUT> operatorFactory,
        TypeInformation<OUT> outputType,
        int parallelism,
        boolean parallelismConfigured) {
    super(name, outputType, parallelism, parallelismConfigured);//super至 Transformation
    this.input = input;
    this.operatorFactory = operatorFactory;
}
```

new SingleOutputStreamOperator

```java
protected SingleOutputStreamOperator(
        StreamExecutionEnvironment environment, Transformation<T> transformation) {
    super(environment, transformation);//super至DataStream
}
```

getExecutionEnvironment().addOperator(resultTransform);  StreamExecutionEnvironment

```java
@Internal
public void addOperator(Transformation<?> transformation) {
    Preconditions.checkNotNull(transformation, "transformation must not be null.");
    this.transformations.add(transformation);
}
```

transformations

```
protected final List<Transformation<?>> transformations = new ArrayList<>();
```

以 KeyedStream和WindowedStream两个转换操作为例，从整体的角度介绍 DataStream API实现。当然还有其他类型的DataStream结构，原理也 基本相似

通过MapReduce算法可以对接入数据按照指定Key进行数据分区， 然后将相同Key值的数据路由到同一分区中，聚合统计算子再基于数据 集进行聚合操作，对于Flink也不例外。在DataStream API中主要是通 过执行keyBy()方法，并指定对应的KeySelector，来实现按照指定Key 对数据进行分区操作，DataStream经过转换后会生成KeyedStream数据 集。当然数据集的物理分区操作并不局限于keyBy()方法，还有其他类 型的物理转换可以实现将DataStream中的数据按照指定的规则路由到 下游的分区，如DataStream.shuffle()分区操作。

KeyedStream设计与实现,根据 KeySelector接口实现类创建KeyedStream数据集，KeySelector接口提 供了getKey()方法，能够从StreamRecord中获取Key字段信息。

DataStream.keyBy()

```java
/**
 * It creates a new {@link KeyedStream} that uses the provided key for partitioning its operator
 * states.
 *
 * @param key The KeySelector to be used for extracting the key for partitioning
 * @return The {@link DataStream} with partitioned state (i.e. KeyedStream)
 */
public <K> KeyedStream<T, K> keyBy(KeySelector<T, K> key) {
    Preconditions.checkNotNull(key);
    return new KeyedStream<>(this, clean(key));
}

/**
 * It creates a new {@link KeyedStream} that uses the provided key with explicit type
 * information for partitioning its operator states.
 *
 * @param key The KeySelector to be used for extracting the key for partitioning.
 * @param keyType The type information describing the key type.
 * @return The {@link DataStream} with partitioned state (i.e. KeyedStream)
 */
public <K> KeyedStream<T, K> keyBy(KeySelector<T, K> key, TypeInformation<K> keyType) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(keyType);
    return new KeyedStream<>(this, clean(key), keyType);
}
```

KeyedStream

```java
public KeyedStream(DataStream<T> dataStream, KeySelector<T, KEY> keySelector) {
    this(
            dataStream,
            keySelector,
            TypeExtractor.getKeySelectorTypes(keySelector, dataStream.getType()));
}
```

```java
public KeyedStream(
        DataStream<T> dataStream,
        KeySelector<T, KEY> keySelector,
        TypeInformation<KEY> keyType) {
    this(
            dataStream,
            new PartitionTransformation<>(
                    dataStream.getTransformation(),
                    new KeyGroupStreamPartitioner<>(
                            keySelector,
                            StreamGraphGenerator.DEFAULT_LOWER_BOUND_MAX_PARALLELISM)),
            keySelector,
            keyType);
}
```

从KeyedStream构造器中可以看出，最终会 创建PartitionTransformation，这里我们称之为物理分区操作，其主 要功能就是对数据元素在上下游算子之间进行重新分区。

在PartitionTransformation的创建过程中会同时构建 KeyGroupStreamPartitioner实例作为参数。 KeyGroupStreamPartitioner是按照Key进行分组发送的分区器。这里 的KeyGroupStreamPartitioner实际上继承了ChannelSelector接口， ChannelSelector主要用于任务执行中，算子根据指定Key的分组信息选择下游节点对应的InputChannel，并将数据元素根据指定Key发送到 下游指定的InputChannel中，最终实现对数据的分区操作。

StreamPartitioner数据分区

KeyGroupStreamPartitioner实际上就是对数据按照Key进行分 组，然后根据Key的分组确定数据被路由到哪个下游的算子中。KeyGroupStreamPartitioner实际上继承自 StreamPartitioner抽象类，而StreamPartitioner又实现了 ChannelSelector接口，用于选择下游的InputChannel。这里可以将其理解为基于Netty中的 channel实现的跨网络数据输入管道，经过网络栈传输的数据最终发送 到指定下游算子的InputChannel中。

根据分区策略的不同，StreamPartitioner 的实现类也有所区别，这些实现类分别被应用在DataStream对应的转 换操作中，例如ShufflePartitioner和DataStream.shuffe()对应

以RebalancePartitioner为例RebalancePartitioner.selectChannel() 方法实现了对InputChannel的选择。在RebalancePartitioner中会记 录nextChannelToSendTo，然后通过(nextChannelToSendTo + 1) % numberOfChannels公式计算并选择下一数据需要发送的 InputChannel。实际上是对所有下游的InputChannel进行轮询，均匀 地将数据发送到下游的Task。

```java
@Override
public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
    nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels;
    return nextChannelToSendTo;
}
```

对于RescalePartitioner来讲，上游Task发送到下游Task的数据 元素取决于上下游之间Task实例的并行度。数据会在本地进行轮询， 然后发送到下游的Task实例中。如果上游Task具有并行性2，而下游 Task具有并行性4，则一个上游Task实例会将元素均匀分配到指定的两 个下游Task实例中;而另一个上游Task将分配给另外两个下游Task。 上游Task的所有数据会在本地对下游的Task进行轮询，然后均匀发送 到已经分配的下游Task实例中。

在RescalePartitioner.selectChannel() 方法中，通过改变nextChannelToSendTo的值选择下一个需要发送的 InputChannel，而方法中的numberOfChannels实际上是根据下游操作 的并行度确定的。

WindowedStream的设计与实现

如果将DataStream根据Key进行分组，生成 KeyedStream数据集，然后在KeyedStream上执行window()转换操作， 就会生成WindowedStream数据集。如果直接调用 DataStream.windowAll()方法进行转换，就会生成AllWindowedStream 数据集。WindowedStream和AllWindowedStream的主要区别在于是否按 照Key进行分区处理，这里我们以WindowedStream为例讲解窗口转换操 作的具体实现。

WindowAssigner设计与实现

当用户调用KeyedStream.window()方法 时，会创建WindowedStream转换操作。通过window()方法可以看出， 此时需要传递WindowAssigner作为窗口数据元素的分配器，通过 WindowAssigner组件，可以根据指定的窗口类型将数据元素分配到指 定的窗口中。

KeyedStream.window()

```java
/**
 * Windows this data stream to a {@code WindowedStream}, which evaluates windows over a key
 * grouped stream. Elements are put into windows by a {@link WindowAssigner}. The grouping of
 * elements is done both by key and by window.
 *
 * <p>A {@link org.apache.flink.streaming.api.windowing.triggers.Trigger} can be defined to
 * specify when windows are evaluated. However, {@code WindowAssigners} have a default {@code
 * Trigger} that is used if a {@code Trigger} is not specified.
 *
 * @param assigner The {@code WindowAssigner} that assigns elements to windows.
 * @return The trigger windows data stream.
 */
@PublicEvolving
public <W extends Window> WindowedStream<T, KEY, W> window(
        WindowAssigner<? super T, W> assigner) {
    return new WindowedStream<>(this, assigner);
}
```

接下来我们看WindowAssigner的具体实现。 WindowAssigner作为抽象类，其子类实现是非常多的，例如基于事件 时间实现的SlidingEventTimeWindows、基于处理时间实现的 TumblingProcessingTimeWindows等。这些WindowAssigner根据窗口类 型进行区分，且属于DataStream API中内置的窗口分配器，用户可以 直接调用它们创建不同类型的窗口转换。

SessionWindow类型的窗口比较特殊，在 WindowAssigner的基础上又实现了MergingWindowAssigner抽象类，在 MergingWindowAssigner抽象类中定义了MergeCallback接口。这样做 的原因是SessionWindow的窗口长度不固定，SessionWindow窗口的长 度取决于指定时间范围内是否有数据元素接入，然后动态地将接入数 据切分成独立的窗口，最后完成窗口计算。此时涉及对窗口中的元素 进行动态Merge操作，这里主要借助MergingWindowAssigner提供的 mergeWindows()方法来实现。

在WindowAssigner中通过提供WindowAssignerContext上下文获取 CurrentProcessingTime等时间信息。在WindowAssigner抽象类中提供 了以下方法供子类选择。

```java
/**
 * A {@code WindowAssigner} assigns zero or more {@link Window Windows} to an element.
 *
 * <p>In a window operation, elements are grouped by their key (if available) and by the windows to
 * which it was assigned. The set of elements with the same key and window is called a pane. When a
 * {@link Trigger} decides that a certain pane should fire the {@link
 * org.apache.flink.streaming.api.functions.windowing.WindowFunction} is applied to produce output
 * elements for that pane.
 *
 * @param <T> The type of elements that this WindowAssigner can assign windows to.
 * @param <W> The type of {@code Window} that this assigner assigns.
 */
@PublicEvolving
public abstract class WindowAssigner<T, W extends Window> implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Returns a {@code Collection} of windows that should be assigned to the element.
     *
     * @param element The element to which windows should be assigned.
     * @param timestamp The timestamp of the element.
     * @param context The {@link WindowAssignerContext} in which the assigner operates.
     */
  //定义将数据元素分配到对应窗口的逻辑。
    public abstract Collection<W> assignWindows(
            T element, long timestamp, WindowAssignerContext context);

    /** Returns the default trigger associated with this {@code WindowAssigner}. */
  //获取默认的Trigger，也就是默认窗口触发 器，例如EventTimeTrigger。
    public abstract Trigger<T, W> getDefaultTrigger(StreamExecutionEnvironment env);

    /**
     * Returns a {@link TypeSerializer} for serializing windows that are assigned by this {@code
     * WindowAssigner}.
     */
  //获取WindowSerializer实现，默认为 TimeWindow.Serializer()。
    public abstract TypeSerializer<W> getWindowSerializer(ExecutionConfig executionConfig);

    /**
     * Returns {@code true} if elements are assigned to windows based on event time, {@code false}
     * otherwise.
     */
  //判断是否为基于EventTime时间类型实现的窗口。
    public abstract boolean isEventTime();

    /**
     * A context provided to the {@link WindowAssigner} that allows it to query the current
     * processing time.
     *
     * <p>This is provided to the assigner by its containing {@link
     * org.apache.flink.streaming.runtime.operators.windowing.WindowOperator}, which, in turn, gets
     * it from the containing {@link org.apache.flink.streaming.runtime.tasks.StreamTask}.
     */
    public abstract static class WindowAssignerContext {

        /** Returns the current processing time. */
        public abstract long getCurrentProcessingTime();
    }
}
```

以SlidingEventTimeWindows为例 SlidingEventTimeWindows.assignWindows()

```java
@Override
public Collection<TimeWindow> assignWindows(
        Object element, long timestamp, WindowAssignerContext context) {
    if (timestamp > Long.MIN_VALUE) {
        List<TimeWindow> windows = new ArrayList<>((int) (size / slide));
        long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
        for (long start = lastStart; start > timestamp - size; start -= slide) {
            windows.add(new TimeWindow(start, start + size));
        }
        return windows;
    } else {
        throw new RuntimeException(
                "Record has Long.MIN_VALUE timestamp (= no timestamp marker). "
                        + "Is the time characteristic set to 'ProcessingTime', or did you forget to call "
                        + "'DataStream.assignTimestampsAndWatermarks(...)'?");
    }
}
```

在SlidingEventTimeWindows.assignWindows()方法中可以看出， assignWindows()方法的参数包含了当前数据元素element、timestamp 和WindowAssignerContext的上下文信息，且方法主要包含如下逻辑。

·判断timestamp是否有效，然后根据窗口长度和滑动时间计算数 据元素所属窗口的数量，再根据窗口数量创建窗口列表。

·调用TimeWindow.getWindowStartWithOffset()方法，确定窗口列表 中最晚的窗口对应的WindowStart时间，并赋值给lastStart变量;然后从 lastStart开始遍历，每次向前移动固定的slide长度;最后向windows窗口 列表中添加创建的TimeWindow，在TimeWindow中需要指定窗口的起始 时间和结束时间。

·返回创建的窗口列表windows，也就是当前数据元素所属的窗口 列表。

创建的WindowAssigner实例会在WindowOperator中使用，输入一 条数据元素时会调用WindowAssigner.assignWindows()方法为接入的 数据元素分配窗口，WindowOperator会根据元素所属的窗口分别对数 据元素进行处理。

当然还有其他类型的WindowAssigner实现，基本功能都是一样 的，主要是根据输入的元素确定和分配窗口。对于SlidingWindow类型 的窗口来讲，同一个数据元素可能属于多个窗口，主要取决于窗口大 小和滑动时间长度;而对于TumpleWindow类型来讲，每个数据元素仅 属于一个窗口。

Window Trigger的核心实现

indow Trigger决定了窗口触发WindowFunction计算的时机，当 接入的数据元素通过WindowAssigner分配到不同的窗口后，数据元素 会被不断地累积在窗口状态中。当满足窗口触发条件时，会取出当前 窗口中的所有数据元素，基于指定的WindowFunction对窗口中的数据 元素进行运算，最后产生窗口计算结果并发送到下游的算子中

所有定义的Window Trigger继承自Trigger基本实现类。每种窗口的触发策略不同，相应 的Trigger触发器也有所不同。例如TumblingProcessingTimeWindows 对应的默认Trigger为ProcessingTimeTrigger，而 SlidingEventTimeWindows默认对应的是EventTimeTrigger。

数据元素接入WindowOperator后，调用窗口触发器的onElement() 方法，判断窗口是否满足触发条件。如果满足，则触发窗口计算操 作。我们以EventTimeTrigger为例介绍Trigger的核心实现，

EventTimeTrigger.onElement()

```java
@Override
public TriggerResult onElement(
        Object element, long timestamp, TimeWindow window, TriggerContext ctx)
        throws Exception {
  //当数据元素接入后，根据窗口中maxTimestamp是否大于当前算 子中的Watermark决定是否触发窗口计算。如果符合触发条件，则返回 TriggerResult.FIRE事件，这里的maxTimestamp实际上是窗口的结束时间 减1，属于该窗口的最大时间戳。
    if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
        // if the watermark is already past the window fire immediately
        return TriggerResult.FIRE;
    } else {
      //当数据元素不断接入WindowOperator，不断更新Watermark时， 只要Watermark大于窗口的右边界就会触发相应的窗口计算
        ctx.registerEventTimeTimer(window.maxTimestamp());
      //如果不满足以上条件，就会继续向TriggerContext中注册Timer定 时器，等待指定的时间再通过定时器触发窗口计算，此时方法会返回 TriggerResult.CONTINUE消息给WindowOperator，表示此时窗口不会触 发计算，继续等待新的数据接入。
        return TriggerResult.CONTINUE;
    }
}
```

在EventTimeTrigger.onElement()方法定义中我们可以看到，当 窗口不满足触发条件时，会向TriggerContext中注册EventTimeTimer 定时器，指定的触发时间为窗口中的最大时间戳。算子中的Watermark 到达该时间戳时，会自动触发窗口计算，不需要等待新的数据元素接 入。这里TriggerContext使用到的TimerService实际上就是介绍过的InternalTimerService，EventTimeTimer会基于 InternalTimerService的实现类进行存储和管理。

当Timer定时器到达maxTimestamp时就会调用 EventTimeTrigger.onEventTime()方法。在 EventTimeTrigger.onEventTime()方法中，实际上会判断传入的事件 时间和窗口的maxTimestamp是否相等，如果相等则返回 TriggerResult.FIRE并触发窗口的统计计算。

EventTimeTrigger.onEventTime()

```java
@Override
public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
    return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
}
```

WindowFunction的设计与实现

经过以上几个步骤，基本上就能够确认窗口的类型及相应的触发时机了。窗口符合触发条件之后，就会对窗口中已经积蓄的数据元素进行统计计算，以得到最终的统计结果。对窗口元素的计算逻辑定义则主要通过窗口函数来实现。

在WindowStream的计算中，将窗口函数分为两种类型:用户指定 的聚合函数AggregateFunction和专门用于窗口计算的 WindowFunction。对于大部分用户来讲，基本都是基于窗口做聚合类 型的统计运算，因此只需要在WindowStream中指定相应的聚合函数， 如ReduceFunction和AggregateFunction。而在WindowStream的计算过 程中，实际上会通过WindowFunction完成更加复杂的窗口计算。

WindowFunction继承了Function接口，同时又被 不同类型的聚合函数实现，例如实现窗口关联计算的 CoGroupWindowFunction、在窗口中对元素进行Reduce操作的 ReduceApplyWindowFunction。这些函数同时继承自 WrappingFunction，WrappingFunction对WindowFunction进行了一层 封装，主要通过继承AbstractRichFunction抽象类，拓展和实现了 RichFunction提供的能力。

总而言之，窗口中的函数会将用户定义的聚合函数和 WindowFunction进行整合，形成统一的RichWindowFunction，然后基 于RichWindowFunction进行后续的操作。

用户创建WindowStream后，将 ReduceFunction传递给WindowStream.reduce()方法。在 WindowStream.reduce()方法中可以看出，还需要将WindowFunction作 为参数，但这里的WindowFunction会在WindowStream中创建 PassThroughWindowFunction默认实现类。

WindowStream.reduce()

```java
public <R> SingleOutputStreamOperator<R> reduce(
        ReduceFunction<T> reduceFunction,
        WindowFunction<T, R, K, W> function,
        TypeInformation<R> resultType) {

    // clean the closures
    function = input.getExecutionEnvironment().clean(function);
    reduceFunction = input.getExecutionEnvironment().clean(reduceFunction);

    final String opName = builder.generateOperatorName();
    final String opDescription = builder.generateOperatorDescription(reduceFunction, function);

    OneInputStreamOperator<T, R> operator = builder.reduce(reduceFunction, function);
    return input.transform(opName, resultType, operator).setDescription(opDescription);
}
```

最后实际上就是创建OneInputStreamOperator实例， StreamOperator会根据evictor数据剔除器是否为空，选择创建 EvictingWindowOperator还是WindowOperator。在创建 EvictingWindowOperator时，通过调用new ReduceApplyWindowFunction <?> (reduceFunction, function)合并 ReduceFunction和WindowFunction，然后转换为 InternalIterableWindowFunction函数供WindowOperator使用。接下 来调用input.transform()方法将创建好的EvictingWindowOperator或 WindowOperator实例添加到OneInputTransformation转换操作中。其 他的窗口计算函数和Reduce聚合函数基本一致