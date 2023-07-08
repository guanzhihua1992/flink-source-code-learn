DataStream之间的转换操作,每种Transformation实现都 和DataStream的接口方法对应。Transformation的实现子类涵盖了所有的 DataStream转换操作。常用到的StreamMap、StreamFilter算子封装在 OneInputTransformation中，也就是单输入类型的转换操作。常见的双输入类型算子有join、connect等，对应支持双输入类型转换的 TwoInputTransformation操作。

另外，在Transformation的基础上又抽象出了 PhysicalTransformation类。PhysicalTransformation中提供了 setChainingStrategy()方法，可以将上下游算子按照指定的策略连 接。ChainingStrategy支持如下策略。

```java
@PublicEvolving
public enum ChainingStrategy {

    /**
     * Operators will be eagerly chained whenever possible.
     *
     * <p>To optimize performance, it is generally a good practice to allow maximal chaining and
     * increase operator parallelism.
     */
  //代表该Transformation中的算子会和上游算子尽可能 地链化，最终将多个Operator组合成OperatorChain。OperatorChain中的 Operator会运行在同一个SubTask实例中，这样做的目的主要是优化性 能，减少Operator之间的网络传输。
    ALWAYS,

    /** The operator will not be chained to the preceding or succeeding operators. */
  //代表该Transformation中的Operator永远不会和上下游 算子之间链化，因此对应的Operator会运行在独立的SubTask实例中。
    NEVER,

    /**
     * The operator will not be chained to the predecessor, but successors may chain to this
     * operator.
     */
  //代表该Transformation对应的Operator为头部算子，不支 持上游算子链化，但是可以和下游算子链化，实际上就是OperatorChain 中的HeaderOperator。
    HEAD,

    /**
     * This operator will run at the head of a chain (similar as in {@link #HEAD}, but it will
     * additionally try to chain source inputs if possible. This allows multi-input operators to be
     * chained with multiple sources into one task.
     */
  //基本和HEAD一致 但是他会尝试着和source inputs链接在一起
    HEAD_WITH_SOURCES;
//默认策略 ALWAYS
    public static final ChainingStrategy DEFAULT_CHAINING_STRATEGY = ALWAYS;
}
```

通过以上策略可以控制算子之间的连接，在生成JobGraph时， ALWAYS类型连接的Operator形成OperatorChain。同一个 OperatorChain中的Operator会运行在同一个SubTask线程中，从而尽 可能地避免网络数据交换，提高计算性能。当然，用户也可以显性调 用disableChaining()等方法，设定不同的ChainingStrategy，实现对 Operator之间物理连接的控制。

以下是支持设定ChainingStrategy的PhysicalTransformation操 作类型，也就是继承了PhysicalTransformation抽象的实现类。

·OneInputTransformation:单进单出的数据集转换操作，例如 DataStream.map()转换。

·TwoInputTransformation:双进单出的数据集转换操作，例如在 DataStream与DataStream之间进行Join操作，且该转换操作中的Operator 类型为TwoInputStreamOperator。

·SinkTransformation:数据集输出操作，当用户调用 DataStream.addSink()方法时，会同步创建SinkTransformation操作，将 DataStream中的数据输出到外部系统中。

.LegacySinkTransformation

·SourceTransformation:数据集输入操作，调用 DataStream.addSource()方法时，会创建SourceTransformation操作，用于 从外部系统中读取数据并转换成DataStream数据集。

.LegacySourceTransformation

·SplitTransformation:数据集切分操作，用于将DataStream数据集 根据指定字段进行切分，调用DataStream.split()方法时会创建 SplitTransformation。

.TimestampsAndWatermarksTransformation

.BroadcastStateTransformation

.KeyedBroadcastStateTransformation

.MultipleInputTransformation

.KeyedMultipleInputTransformation

.ReduceTransformation

除了PhysicalTransformation之外，还有一部分转换操作直接继 承自Transformation抽象类，这些Transformation本身就是物理转换 操作，不支持链化操作，因此不会将其与其他算子放置在同一个 SubTask中运行。例如PartitionTransformation和 SelectTransformation等转换操作，这类转换操作不涉及具体的数据 处理过程，仅描述上下游算子之间的数据分区。

·SelectTransformation:根据用户提供的selectedName从上游 DataStream中选择需要输出到下游的数据。

·PartitionTransformation:支持对上游DataStream中的数据进行分 区，分区策略通过指定的StreamPartitioner决定，例如当用户执行 DataStream.rebalance()方法时，就会创建StreamPartitioner实现类 RebalancePartitioner实现上下游数据的路由操作。

·UnionTransformation:用于对多个输入Transformation进行合并， 最终将上游DataStream数据集中的数据合并为一个DataStream。

·SideOutputTransformation:用于根据OutputTag筛选上游 DataStream中的数据并下发到下游的算子中继续处理。

·CoFeedbackTransformation:用于迭代计算中单输入反馈数据流 节点的转换操作。

·FeedbackTransformation:用于迭代计算中双输入反馈数据流节点 的转换操作。