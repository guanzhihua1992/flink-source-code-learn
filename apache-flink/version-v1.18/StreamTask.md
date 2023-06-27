StreamTask通过继承和实现AbstractInvokable抽象类，提供了对 流计算任务的支持和实现，StreamTask是所有Streaming类型Task的基 本实现类。StreamTask最终会被运行在TaskExecutor创建的Task线程 中，触发StreamTask操作主要借助AbstractInvokable实现类中的 invoke()方法。Task是TaskManager中部署和执行的最小本地执行单 元，StreamTask定义了Task线程内部需要执行的逻辑，在StreamTask 中包含了一个或者多个Operator，其中多个Operator会被链化成 Operator Chain，运行在相同的Task实例中。

StreamTask的常见实现类有 OneInputStreamTask、TwoInputStreamTask以及SourceStreamTask 等，且这些子类全部实现了StreamTask.init()抽象方法，并在init() 方法中初始化StreamTask需要的变量和服务等。其他类型的 StreamTask如StreamIterationHead和StreamIterationTail主要用于 迭代计算，BoundedStreamTask则主要在state-processing-api中使 用。

StreamTask的主要成员变量及概念。

·headOperator:指定当前StreamTask的头部算子，即operatorChain 中的第一个算子，StreamTask负责从输入数据流中接入数据，并传递给 operatorChain中的headOperator继续处理。

·operatorChain:在StreamTask构建过程中会合并Operator，形成 operatorChain，operatorChain中的所有Operator都会被执行在同一个Task 实例中。

·stateBackend:StreamTask执行过程中使用到状态后端，用于管理 该StreamTask运行过程中产生的状态数据。

·checkpointStorage:用于对Checkpoint过程中的状态数据进行外部 持久化。

·timerService:Task执行过程中用到的定时器服务。

·accumulatorMap:用于存储累加器数据，用户自定义的累加器会 存放在accumulatorMap中。

·asyncOperationsThreadPool:Checkpoint操作对状态数据进行快照 操作时所用的异步线程池，目的是避免Checkpoint操作阻塞主线程的计算任务。

·mailboxProcessor:采用类似Actor模型的邮箱机制取代之前的多 线程模型，让Task执行的过程变为单线程(mailbox线程)加阻塞队列 的形式。从而更好地解决由于多线程加对象锁带来的问题。

当Task线程调用AbstractInvokable.invoke()方 法触发执行StreamTask后，在StreamTask中会将整个Invoke过程分为 三个阶段:BeforeInvoke、Run以及AfterInvoke。在BeforeInvoke阶 段会准备当前Task执行需要的环境信息，例如StateBackend的创建 等。Run阶段对应的是开启Operator，正式启动Operator逻辑。当Task 执行完毕后，会在AfterInvoke阶段处理Task停止的操作，例如关闭 Operator、释放Operator等。

从Task 中runWithSystemExitMonitoring(finalInvokable::invoke); 开始

```java
@Override
public final void invoke() throws Exception {
    // Allow invoking method 'invoke' without having to call 'restore' before it.
    if (!isRunning) {
        LOG.debug("Restoring during invoke will be called.");
        restoreInternal();
    }

    // final check to exit early before starting to run
    ensureNotCanceled();

    scheduleBufferDebloater();

    // let the task do its work
    getEnvironment().getMetricGroup().getIOMetricGroup().markTaskStart();
  //调用runMailboxLoop()方法执行Operator中的计算逻辑。
    runMailboxLoop();

    // if this left the run() method cleanly despite the fact that this was canceled,
    // make sure the "clean shutdown" is not attempted
    ensureNotCanceled();
//当Task执行完毕后调用afterInvoke()方法，清理和关闭 Operator。
    afterInvoke();
}
```

restoreInternal();

```java
void restoreInternal() throws Exception {
    if (isRunning) {
        LOG.debug("Re-restore attempt rejected.");
        return;
    }
    isRestoring = true;
    closedOperators = false;
    LOG.debug("Initializing {}.", getName());

    operatorChain =
            getEnvironment().getTaskStateManager().isTaskDeployedAsFinished()
                    ? new FinishedOperatorChain<>(this, recordWriter)
                    : new RegularOperatorChain<>(this, recordWriter);
    mainOperator = operatorChain.getMainOperator();

    getEnvironment()
            .getTaskStateManager()
            .getRestoreCheckpointId()
            .ifPresent(restoreId -> latestReportCheckpointId = restoreId);

    // task specific initialization
    init();

    // save the work of reloading state, etc, if the task is already canceled
    ensureNotCanceled();

    // -------- Invoke --------
    LOG.debug("Invoking {}", getName());

    // we need to make sure that any triggers scheduled in open() cannot be
    // executed before all operators are opened
    CompletableFuture<Void> allGatesRecoveredFuture = actionExecutor.call(this::restoreGates);

    // Run mailbox until all gates will be recovered.
    mailboxProcessor.runMailboxLoop();

    ensureNotCanceled();

    checkState(
            allGatesRecoveredFuture.isDone(),
            "Mailbox loop interrupted before recovery was finished.");

    // we recovered all the gates, we can close the channel IO executor as it is no longer
    // needed
    channelIOExecutor.shutdown();

    isRunning = true;
    isRestoring = false;
}
```

restoreGates

```java
private CompletableFuture<Void> restoreGates() throws Exception {
    SequentialChannelStateReader reader =
            getEnvironment().getTaskStateManager().getSequentialChannelStateReader();
    reader.readOutputData(
            getEnvironment().getAllWriters(), !configuration.isGraphContainingLoops());

    operatorChain.initializeStateAndOpenOperators(createStreamTaskStateInitializer());

    IndexedInputGate[] inputGates = getEnvironment().getAllInputGates();
    channelIOExecutor.execute(
            () -> {
                try {
                    reader.readInputData(inputGates);
                } catch (Exception e) {
                    asyncExceptionHandler.handleAsyncException(
                            "Unable to read channel state", e);
                }
            });

    // We wait for all input channel state to recover before we go into RUNNING state, and thus
    // start checkpointing. If we implement incremental checkpointing of input channel state
    // we must make sure it supports CheckpointType#FULL_CHECKPOINT
    List<CompletableFuture<?>> recoveredFutures = new ArrayList<>(inputGates.length);
    for (InputGate inputGate : inputGates) {
        recoveredFutures.add(inputGate.getStateConsumedFuture());

        inputGate
                .getStateConsumedFuture()
                .thenRun(
                        () ->
                                mainMailboxExecutor.execute(
                                        inputGate::requestPartitions,
                                        "Input gate request partitions"));
    }

    return CompletableFuture.allOf(recoveredFutures.toArray(new CompletableFuture[0]))
            .thenRun(mailboxProcessor::suspend);
}
```

```java
operatorChain.initializeStateAndOpenOperators(createStreamTaskStateInitializer());
```

```java
@Override
public void initializeStateAndOpenOperators(
        StreamTaskStateInitializer streamTaskStateInitializer) throws Exception {
    for (StreamOperatorWrapper<?, ?> operatorWrapper : getAllOperators(true)) {
        StreamOperator<?> operator = operatorWrapper.getStreamOperator();
      //分别调用每个Operator中的initializeState()和open()方法，完成对各个Operator状态的初始化和算子初始化操作
        operator.initializeState(streamTaskStateInitializer);
        operator.open();
    }
}
```

StreamTask中的算子初始化完毕后，正式进入数据处理阶段，此 时将来自外部的数据写入StreamTask中的Operator进行处理，对接入 的数据或事件进行处理主要借助StreamTask中线程模型完成。如以下 代码所示，在StreamTask的构造器中创建MailboxProcessor实例，同 时将StreamTask.processInput()作为方法块传递给 MailboxProcessor，作为MailboxDefaultActition的默认执行逻辑。 在StreamTask.processInput()方法中定义了当前Task接入外部数据并 进行处理和调用的逻辑。换句话讲，就是触发StreamTask.Invoke()方 法后，会调用StreamTask.runMailboxLoop()方法启动 MailboxProcessor接入和处理数据。

·Mail:定义具体算子中的可执行操作，Mail主要使用 RunnableWithException作为参数，和Runnable类似，用于捕获需要执行 的代码，但和Runnable不同的是，RunnableWithException允许抛出检查异常。同时在Mail中还有priority参数控制Mail执行的优先级，以防止出 现死锁的情况。

·MailboxDefaultAction:定义了当前StreamTask的默认数据处理逻 辑，包括对Watermark等事件以及StreamElement数据元素的处理。在创 建StreamTask的过程中会将StreamTask.processInput()方法作为创建 MailboxProcessor中的MailboxDefaultAction操作，在 StreamTask.processInput()方法中定义了具体数据元素接入和处理的逻 辑。

·MalboxProcessor:定义了Mailbox中的Mail和MailboxDefaultAction 之间的处理逻辑，在runMailboxLoop()循环方法中处理Mail和 DefaultAction。同时在MailboxProcessor中提供了MailboxController用于控 制Mailbox中的LoopRunning以及暂停和恢复MailboxDefaultAction。

·MailboxExecutor:提供了直接向TaskMailbox提交Mail的操作，例 如StreamTask中触发Checkpoint的同时会将具体的执行方法提交到 Mailbox中调用和处理。在MailboxExecutor中也提供了yield()方法从 TaskMailbox中获取Mail并执行，这里主要用于执行SavePoint操作，调用 yield()方法将TaskMailbox中的Mail逐步执行完毕后停止任务。

·TaskMailbox:主要用于存储提交的Mail并提供获取接口， TaskMailbox主要含有两个队列，分别为queue和batch。其中queue是一个 阻塞队列，通过ReentrantLock控制队列的读写操作，而batch是一个非阻 塞队列，当调用createBatch()方法时会将queue中的Mail存储到batch中， 这样读操作就通过调用tryTakeFromBatch()方法批量获取到Mail，且只能 被mailbox thread消费。

Mail中的Runnable Command主要来自默认的 MailboxDefaultAction以及StreamTask中的其他操作，例如 TriggerCheckpoint、NotifyCheckpointComplete等操作。同时， AsyncWaitOperator算子也会向MailBox提交相关的异步执行操作。

```java
// Run mailbox until all gates will be recovered.
mailboxProcessor.runMailboxLoop();
```

```java
/**
 * Runs the mailbox processing loop. This is where the main work is done. This loop can be
 * suspended at any time by calling {@link #suspend()}. For resuming the loop this method should
 * be called again.
 */
public void runMailboxLoop() throws Exception {
    suspended = !mailboxLoopRunning;
//获取最新的TaskMailbox并设定为本地TaskMailbox。
    final TaskMailbox localMailbox = mailbox;

    checkState(
            localMailbox.isMailboxThread(),
            "Method must be executed by declared mailbox thread!");
//检查MailBox线程是否为MailboxThread，确认 TaskMailbox.State是否为开启状态。
    assert localMailbox.getState() == TaskMailbox.State.OPEN : "Mailbox must be opened!";
//将当前MailboxProcessor实例作为参数创建 MailboxController，通过MailboxController实现对Mailbox的循环控 制以及对MailboxDefaultAction的暂停和恢复操作。
    final MailboxController mailboxController = new MailboxController(this);
//启动processMail(localMailbox)循环，如果processMail()方 法一直返回True，则MailboxThread会一直处于循环执行模式，从而调 用mailboxDefaultAction.runDefaultAction(defaultActionContext) 方法处理接入的数据，在runDefaultAction()方法中调用 StreamTask.processInput()方法持续接入数据并处理。
    while (isNextLoopPossible()) {
        // The blocking `processMail` call will not return until default action is available.
        processMail(localMailbox, false);
        if (isNextLoopPossible()) {
            mailboxDefaultAction.runDefaultAction(
                    mailboxController); // lock is acquired inside default action as needed
        }
    }
}
```

MailboxProcessor.processMail()方法主要用于检测MailBox中是 否还有Mail正在处理，只要在MailBox中有Mail，该方法会一直等待 Mail全部处理完毕后再返回。对于流式任务来讲，每次处理完直接返 回True进行下一次MailBox的处理processMail(localMailbox, false);

```java
/**
 * This helper method handles all special actions from the mailbox. In the current design, this
 * method also evaluates all control flag changes. This keeps the hot path in {@link
 * #runMailboxLoop()} free from any other flag checking, at the cost that all flag changes must
 * make sure that the mailbox signals mailbox#hasMail.
 *
 * @return true if a mail has been processed.
 */
private boolean processMail(TaskMailbox mailbox, boolean singleStep) throws Exception {
    // Doing this check is an optimization to only have a volatile read in the expected hot
    // path, locks are only
    // acquired after this point.
    boolean isBatchAvailable = mailbox.createBatch();

    // Take mails in a non-blockingly and execute them.
    boolean processed = isBatchAvailable && processMailsNonBlocking(singleStep);
    if (singleStep) {
        return processed;
    }

    // If the default action is currently not available, we can run a blocking mailbox execution
    // until the default action becomes available again.
    processed |= processMailsWhenDefaultActionUnavailable();

    return processed;
}
```

processMailsWhenDefaultActionUnavailable();

```java
private boolean processMailsWhenDefaultActionUnavailable() throws Exception {
    boolean processedSomething = false;
    Optional<Mail> maybeMail;
    while (!isDefaultActionAvailable() && isNextLoopPossible()) {
        maybeMail = mailbox.tryTake(MIN_PRIORITY);
        if (!maybeMail.isPresent()) {
            maybeMail = Optional.of(mailbox.take(MIN_PRIORITY));
        }
        maybePauseIdleTimer();

        runMail(maybeMail.get());

        maybeRestartIdleTimer();
        processedSomething = true;
    }
    return processedSomething;
}
```

runMail(maybeMail.get());

```java
private void runMail(Mail mail) throws Exception {
    mailboxMetricsControl.getMailCounter().inc();
    mail.run();
    if (!suspended) {
        // start latency measurement on first mail that is not suspending mailbox execution,
        // i.e., on first non-poison mail, otherwise latency measurement is not started to avoid
        // overhead
        if (!mailboxMetricsControl.isLatencyMeasurementStarted()
                && mailboxMetricsControl.isLatencyMeasurementSetup()) {
            mailboxMetricsControl.startLatencyMeasurement();
        }
    }
}
```

扔了一个runnable接口过来

```java
public void run() throws Exception {
    actionExecutor.runThrowing(runnable);
}
```

回到mailboxDefaultAction.runDefaultAction(mailboxController); 

```java
this.mailboxProcessor =
        new MailboxProcessor(
                this::processInput, mailbox, actionExecutor, mailboxMetricsControl);
```

StreamTask.processInput()方法中，实 际上会调用StreamInputProcessor.processInput()方法处理输入的数 据，且处理完成后会返回InputStatus状态，并根据InputStatus的结 果判断是否需要结束当前的Task。通常情况下如果还有数据需要继续 处理，则InputStatus处于MORE_AVAILABLE状态，当InputStatus为 END_OF_INPUT状态，表示数据已经消费完毕，需要终止当前的 MailboxDefaultAction。

InputStatus主要有三种状态，分别为MORE_AVAILABLE、 NOTHING_AVAILABLE和END_OF_INPUT。

·MORE_AVAILABLE:表示在输入数据中还有更多的数据可以消 费，当任务正常运行时，会一直处于MORE_AVAILABLE状态。

·NOTHING_AVAILABLE:表示当前没有数据可以消费，但是未 来会有数据待处理，此时线程模型中的处理线程会被挂起并等待数据 接入。

·END_OF_INPUT:表示数据已经达到最后的状态，之后不再有 数据输入，也预示着整个Task终止。

```java
/**
 * This method implements the default action of the task (e.g. processing one event from the
 * input). Implementations should (in general) be non-blocking.
 *
 * @param controller controller object for collaborative interaction between the action and the
 *     stream task.
 * @throws Exception on any problems in the action.
 */
protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
  // 调用inputProcessor.processInput()方法处理数据
    DataInputStatus status = inputProcessor.processInput();
    switch (status) {
        case MORE_AVAILABLE:
            if (taskIsAvailable()) {
                return;
            }
            break;
        case NOTHING_AVAILABLE:
            break;
        case END_OF_RECOVERY:
            throw new IllegalStateException("We should not receive this event here.");
        case STOPPED:
            endData(StopMode.NO_DRAIN);
            return;
        case END_OF_DATA:
            endData(StopMode.DRAIN);
            return;
        case END_OF_INPUT:
            // Suspend the mailbox processor, it would be resumed in afterInvoke and finished
            // after all records processed by the downstream tasks. We also suspend the default
            // actions to avoid repeat executing the empty default operation (namely process
            // records).
        // 状态为END_OF_INPUT时，调用controller挂起Task作业
            controller.suspendDefaultAction();
            mailboxProcessor.suspend();
            return;
    }
//加了指标数据
    TaskIOMetricGroup ioMetrics = getEnvironment().getMetricGroup().getIOMetricGroup();
    PeriodTimer timer;
    CompletableFuture<?> resumeFuture;
    if (!recordWriter.isAvailable()) {
        timer = new GaugePeriodTimer(ioMetrics.getSoftBackPressuredTimePerSecond());
        resumeFuture = recordWriter.getAvailableFuture();
    } else if (!inputProcessor.isAvailable()) {
        timer = new GaugePeriodTimer(ioMetrics.getIdleTimeMsPerSecond());
        resumeFuture = inputProcessor.getAvailableFuture();
    } else if (changelogWriterAvailabilityProvider != null) {
        // currently, waiting for changelog availability is reported as busy
        // todo: add new metric (FLINK-24402)
        timer = null;
        resumeFuture = changelogWriterAvailabilityProvider.getAvailableFuture();
    } else {
        // data availability has changed in the meantime; retry immediately
        return;
    }
    assertNoException(
            resumeFuture.thenRun(
                    new ResumeWrapper(controller.suspendDefaultAction(timer), timer)));
}
```

DataInputStatus status = inputProcessor.processInput(); StreamOneInputProcessor和StreamMultipleInputProcessor

```java
@Override
public DataInputStatus processInput() throws Exception {
    DataInputStatus status = input.emitNext(output);

    if (status == DataInputStatus.END_OF_DATA) {
        endOfInputAware.endInput(input.getInputIndex() + 1);
        output = new FinishedDataOutput<>();
    } else if (status == DataInputStatus.END_OF_RECOVERY) {
        if (input instanceof RecoverableStreamTaskInput) {
            input = ((RecoverableStreamTaskInput<IN>) input).finishRecovery();
        }
        return DataInputStatus.MORE_AVAILABLE;
    }

    return status;
}
```

DataInputStatus status = input.emitNext(output);

```java
@Override
public DataInputStatus emitNext(DataOutput<T> output) throws Exception {

    while (true) {
        // get the stream element from the deserializer
        if (currentRecordDeserializer != null) {
            RecordDeserializer.DeserializationResult result;
            try {
                result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
            } catch (IOException e) {
                throw new IOException(
                        String.format("Can't get next record for channel %s", lastChannel), e);
            }
            if (result.isBufferConsumed()) {
                currentRecordDeserializer = null;
            }

            if (result.isFullRecord()) {
                processElement(deserializationDelegate.getInstance(), output);
                if (canEmitBatchOfRecords.check()) {
                    continue;
                }
                return DataInputStatus.MORE_AVAILABLE;
            }
        }

        Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();
        if (bufferOrEvent.isPresent()) {
            // return to the mailbox after receiving a checkpoint barrier to avoid processing of
            // data after the barrier before checkpoint is performed for unaligned checkpoint
            // mode
            if (bufferOrEvent.get().isBuffer()) {
                processBuffer(bufferOrEvent.get());
            } else {
                DataInputStatus status = processEvent(bufferOrEvent.get());
                if (status == DataInputStatus.MORE_AVAILABLE && canEmitBatchOfRecords.check()) {
                    continue;
                }
                return status;
            }
        } else {
            if (checkpointedInputGate.isFinished()) {
                checkState(
                        checkpointedInputGate.getAvailableFuture().isDone(),
                        "Finished BarrierHandler should be available");
                return DataInputStatus.END_OF_INPUT;
            }
            return DataInputStatus.NOTHING_AVAILABLE;
        }
    }
}
```

processElement(deserializationDelegate.getInstance(), output);

```java
private void processElement(StreamElement recordOrMark, DataOutput<T> output) throws Exception {
    if (recordOrMark.isRecord()) {
        output.emitRecord(recordOrMark.asRecord());
    } else if (recordOrMark.isWatermark()) {
        statusWatermarkValve.inputWatermark(
                recordOrMark.asWatermark(), flattenedChannelIndices.get(lastChannel), output);
    } else if (recordOrMark.isLatencyMarker()) {
        output.emitLatencyMarker(recordOrMark.asLatencyMarker());
    } else if (recordOrMark.isWatermarkStatus()) {
        statusWatermarkValve.inputWatermarkStatus(
                recordOrMark.asWatermarkStatus(),
                flattenedChannelIndices.get(lastChannel),
                output);
    } else {
        throw new UnsupportedOperationException("Unknown type of StreamElement");
    }
}
```