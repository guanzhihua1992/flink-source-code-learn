xecution将TaskDeploymentDescriptor信息提交到TaskManager 之后，接下来在TaskManager中根据TaskDeploymentDescriptor的信息 启动Task实例。如代码清单4-39所示，TaskExecutor.submitTask()方 法根据Execution提交的TaskDeploymentDescriptor信息创建并运行 Task线程。
首先在TaskExecutor中根据配置信息创建Task线程，然后调用 taskSlotTable.addTask(task)方法将创建的Task添加到 taskSlotTable集合中。从submitTask()方法可以看出，Task线程中的 参数一部分来自TaskDeploymentDescriptor(tdd)，另外一部分来自 TaskExecutorService服务，其中包括从taskExecutorServices中获取 的IOManager、ShuffleEnvironment以及KvStateService等。当然还有 其他比较重要的组件和服务，如checkpointResponder和libraryCache 等，这些都是直接从TaskExecutor中获取的。

提交taskManagerGateway.submitTask(deployment, rpcTimeout)

```java
@Override
public CompletableFuture<Acknowledge> submitTask(TaskDeploymentDescriptor tdd, Time timeout) {
    return taskExecutorGateway.submitTask(tdd, jobMasterId, timeout);
}
```

TaskExecutor.submitTask

```java
@Override
public CompletableFuture<Acknowledge> submitTask(
        TaskDeploymentDescriptor tdd, JobMasterId jobMasterId, Time timeout) {

    try {
        final JobID jobId = tdd.getJobId();
        final ExecutionAttemptID executionAttemptID = tdd.getExecutionAttemptId();

        final JobTable.Connection jobManagerConnection =
                jobTable.getConnection(jobId)
                        .orElseThrow(
                                () -> {
                                    final String message =
                                            "Could not submit task because there is no JobManager "
                                                    + "associated for the job "
                                                    + jobId
                                                    + '.';

                                    log.debug(message);
                                    return new TaskSubmissionException(message);
                                });

        if (!Objects.equals(jobManagerConnection.getJobMasterId(), jobMasterId)) {
            final String message =
                    "Rejecting the task submission because the job manager leader id "
                            + jobMasterId
                            + " does not match the expected job manager leader id "
                            + jobManagerConnection.getJobMasterId()
                            + '.';

            log.debug(message);
            throw new TaskSubmissionException(message);
        }

        if (!taskSlotTable.tryMarkSlotActive(jobId, tdd.getAllocationId())) {
            final String message =
                    "No task slot allocated for job ID "
                            + jobId
                            + " and allocation ID "
                            + tdd.getAllocationId()
                            + '.';
            log.debug(message);
            throw new TaskSubmissionException(message);
        }

        // re-integrate offloaded data:
        try {
            tdd.loadBigData(taskExecutorBlobService.getPermanentBlobService());
        } catch (IOException | ClassNotFoundException e) {
            throw new TaskSubmissionException(
                    "Could not re-integrate offloaded TaskDeploymentDescriptor data.", e);
        }

        // deserialize the pre-serialized information
        final JobInformation jobInformation;
        final TaskInformation taskInformation;
        try {
            jobInformation =
                    tdd.getSerializedJobInformation()
                            .deserializeValue(getClass().getClassLoader());
            taskInformation =
                    tdd.getSerializedTaskInformation()
                            .deserializeValue(getClass().getClassLoader());
        } catch (IOException | ClassNotFoundException e) {
            throw new TaskSubmissionException(
                    "Could not deserialize the job or task information.", e);
        }

        if (!jobId.equals(jobInformation.getJobId())) {
            throw new TaskSubmissionException(
                    "Inconsistent job ID information inside TaskDeploymentDescriptor ("
                            + tdd.getJobId()
                            + " vs. "
                            + jobInformation.getJobId()
                            + ")");
        }

        TaskManagerJobMetricGroup jobGroup =
                taskManagerMetricGroup.addJob(
                        jobInformation.getJobId(), jobInformation.getJobName());

        // note that a pre-existing job group can NOT be closed concurrently - this is done by
        // the same TM thread in removeJobMetricsGroup
        TaskMetricGroup taskMetricGroup =
                jobGroup.addTask(tdd.getExecutionAttemptId(), taskInformation.getTaskName());

        InputSplitProvider inputSplitProvider =
                new RpcInputSplitProvider(
                        jobManagerConnection.getJobManagerGateway(),
                        taskInformation.getJobVertexId(),
                        tdd.getExecutionAttemptId(),
                        taskManagerConfiguration.getRpcTimeout());

        final TaskOperatorEventGateway taskOperatorEventGateway =
                new RpcTaskOperatorEventGateway(
                        jobManagerConnection.getJobManagerGateway(),
                        executionAttemptID,
                        (t) -> runAsync(() -> failTask(executionAttemptID, t)));

        TaskManagerActions taskManagerActions = jobManagerConnection.getTaskManagerActions();
        CheckpointResponder checkpointResponder = jobManagerConnection.getCheckpointResponder();
        GlobalAggregateManager aggregateManager =
                jobManagerConnection.getGlobalAggregateManager();

        LibraryCacheManager.ClassLoaderHandle classLoaderHandle =
                jobManagerConnection.getClassLoaderHandle();
        PartitionProducerStateChecker partitionStateChecker =
                jobManagerConnection.getPartitionStateChecker();

        final TaskLocalStateStore localStateStore =
                localStateStoresManager.localStateStoreForSubtask(
                        jobId,
                        tdd.getAllocationId(),
                        taskInformation.getJobVertexId(),
                        tdd.getSubtaskIndex(),
                        taskManagerConfiguration.getConfiguration(),
                        jobInformation.getJobConfiguration());

        // TODO: Pass config value from user program and do overriding here.
        final StateChangelogStorage<?> changelogStorage;
        try {
            changelogStorage =
                    changelogStoragesManager.stateChangelogStorageForJob(
                            jobId,
                            taskManagerConfiguration.getConfiguration(),
                            jobGroup,
                            localStateStore.getLocalRecoveryConfig());
        } catch (IOException e) {
            throw new TaskSubmissionException(e);
        }

        final JobManagerTaskRestore taskRestore = tdd.getTaskRestore();

        final TaskStateManager taskStateManager =
                new TaskStateManagerImpl(
                        jobId,
                        tdd.getExecutionAttemptId(),
                        localStateStore,
                        changelogStorage,
                        changelogStoragesManager,
                        taskRestore,
                        checkpointResponder);

        MemoryManager memoryManager;
        try {
            memoryManager = taskSlotTable.getTaskMemoryManager(tdd.getAllocationId());
        } catch (SlotNotFoundException e) {
            throw new TaskSubmissionException("Could not submit task.", e);
        }

        Task task =
                new Task(
                        jobInformation,
                        taskInformation,
                        tdd.getExecutionAttemptId(),
                        tdd.getAllocationId(),
                        tdd.getProducedPartitions(),
                        tdd.getInputGates(),
                        memoryManager,
                        sharedResources,
                        taskExecutorServices.getIOManager(),
                        taskExecutorServices.getShuffleEnvironment(),
                        taskExecutorServices.getKvStateService(),
                        taskExecutorServices.getBroadcastVariableManager(),
                        taskExecutorServices.getTaskEventDispatcher(),
                        externalResourceInfoProvider,
                        taskStateManager,
                        taskManagerActions,
                        inputSplitProvider,
                        checkpointResponder,
                        taskOperatorEventGateway,
                        aggregateManager,
                        classLoaderHandle,
                        fileCache,
                        taskManagerConfiguration,
                        taskMetricGroup,
                        partitionStateChecker,
                        getRpcService().getScheduledExecutor(),
                        channelStateExecutorFactoryManager.getOrCreateExecutorFactory(jobId));

        taskMetricGroup.gauge(MetricNames.IS_BACK_PRESSURED, task::isBackPressured);

        log.info(
                "Received task {} ({}), deploy into slot with allocation id {}.",
                task.getTaskInfo().getTaskNameWithSubtasks(),
                tdd.getExecutionAttemptId(),
                tdd.getAllocationId());

        boolean taskAdded;

        try {
            taskAdded = taskSlotTable.addTask(task);
        } catch (SlotNotFoundException | SlotNotActiveException e) {
            throw new TaskSubmissionException("Could not submit task.", e);
        }

        if (taskAdded) {
          // 启动Task线程
            task.startTaskThread();
// 设定ResultPartition
            setupResultPartitionBookkeeping(
                    tdd.getJobId(), tdd.getProducedPartitions(), task.getTerminationFuture());
            return CompletableFuture.completedFuture(Acknowledge.get());
        } else {
            final String message =
                    "TaskManager already contains a task for id " + task.getExecutionId() + '.';

            log.debug(message);
            throw new TaskSubmissionException(message);
        }
    } catch (TaskSubmissionException e) {
        return FutureUtils.completedExceptionally(e);
    }
}
```

taskAdded = taskSlotTable.addTask(task);  task.startTaskThread();

```java
/** Starts the task's thread. */
public void startTaskThread() {
    executingThread.start();//线程启动
}
```

```java
/** The core work method that bootstraps the task and executes its code. */
@Override
public void run() {
    try {
        doRun();//逻辑在doRun
    } finally {
        terminationFuture.complete(executionState);
    }
}
```

doRun()

```java
private void doRun() {
    // ----------------------------
    //  Initial State transition
    // ----------------------------
    while (true) {//线程 while(true) 状态判断
        ExecutionState current = this.executionState;
        if (current == ExecutionState.CREATED) {
            if (transitionState(ExecutionState.CREATED, ExecutionState.DEPLOYING)) {
                // success, we can start our work
                break;
            }
        } else if (current == ExecutionState.FAILED) {
            // we were immediately failed. tell the TaskManager that we reached our final state
            notifyFinalState();
            if (metrics != null) {
                metrics.close();
            }
            return;
        } else if (current == ExecutionState.CANCELING) {
            if (transitionState(ExecutionState.CANCELING, ExecutionState.CANCELED)) {
                // we were immediately canceled. tell the TaskManager that we reached our final
                // state
                notifyFinalState();
                if (metrics != null) {
                    metrics.close();
                }
                return;
            }
        } else {
            if (metrics != null) {
                metrics.close();
            }
            throw new IllegalStateException(
                    "Invalid state for beginning of operation of task " + this + '.');
        }
    }

    // all resource acquisitions and registrations from here on
    // need to be undone in the end
    Map<String, Future<Path>> distributedCacheEntries = new HashMap<>();
    TaskInvokable invokable = null;

    try {
        // ----------------------------
        //  Task Bootstrap - We periodically
        //  check for canceling as a shortcut
        // ----------------------------

        // activate safety net for task thread
      // 激活当前Task线程的安全网
        LOG.debug("Creating FileSystem stream leak safety net for task {}", this);
        FileSystemSafetyNet.initializeSafetyNetForThread();

        // first of all, get a user-code classloader
        // this may involve downloading the job's JAR files and/or classes
        LOG.info("Loading JAR files for task {}.", this);
// 为当前Task加载依赖JAR包，然后创建UserCodeClassLoader
        userCodeClassLoader = createUserCodeClassloader();
      // 反序列化ExecutionConfig
        final ExecutionConfig executionConfig =
               serializedExecutionConfig.deserializeValue(userCodeClassLoader.asClassLoader());
        Configuration executionConfigConfiguration = executionConfig.toConfiguration();
// 获取taskCancellationInterval和taskCancellationTimeout参数
        // override task cancellation interval from Flink config if set in ExecutionConfig
        taskCancellationInterval =
                executionConfigConfiguration
                        .getOptional(TaskManagerOptions.TASK_CANCELLATION_INTERVAL)
                        .orElse(taskCancellationInterval);

        // override task cancellation timeout from Flink config if set in ExecutionConfig
        taskCancellationTimeout =
                executionConfigConfiguration
                        .getOptional(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT)
                        .orElse(taskCancellationTimeout);

        if (isCanceledOrFailed()) {
            throw new CancelTaskException();
        }

        // ----------------------------------------------------------------
        // register the task with the network stack
        // this operation may fail if the system does not have enough
        // memory to run the necessary data exchanges
        // the registration must also strictly be undone
        // ----------------------------------------------------------------

        LOG.debug("Registering task at network: {}.", this);
//ResultPartitions和InputGates等组件的配置
        setupPartitionsAndGates(partitionWriters, inputGates);

        for (ResultPartitionWriter partitionWriter : partitionWriters) {
            taskEventDispatcher.registerPartition(partitionWriter.getPartitionId());
        }

        // next, kick off the background copying of files for the distributed cache
        try {
            for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry :
                    DistributedCache.readFileInfoFromConfig(jobConfiguration)) {
                LOG.info("Obtaining local cache file for '{}'.", entry.getKey());
                Future<Path> cp =
                        fileCache.createTmpFile(
                                entry.getKey(), entry.getValue(), jobId, executionId);
                distributedCacheEntries.put(entry.getKey(), cp);
            }
        } catch (Exception e) {
            throw new Exception(
                    String.format(
                            "Exception while adding files to distributed cache of task %s (%s).",
                            taskNameWithSubtask, executionId),
                    e);
        }

        if (isCanceledOrFailed()) {
            throw new CancelTaskException();
        }

        // ----------------------------------------------------------------
        //  call the user code initialization methods
        // ----------------------------------------------------------------
//TaskKvStateRegistry服务的启动和初始化
        TaskKvStateRegistry kvStateRegistry =
                kvStateService.createKvStateTaskRegistry(jobId, getJobVertexId());
//创建 RuntimeEnvironment实例 Task线程中触发算子执行时会将 RuntimeEnvironment环境信息传递给算子构造器，将Task线程实例中 已经初始化的配置和服务提供给算子使用。从RuntimeEnvironment的 构造器中可以看出，这里包含了非常多的配置信息和组件服务，例如 jobConfiguration、taskConfiguration等基础配置，还有 taskStateManager、aggregateManager以及 broadcastVariableManager等组件服务。
      //创建RuntimeEnvironment对象，用于向 Operator提供Task线程的环境信息
        Environment env =
                new RuntimeEnvironment(
                        jobId,
                        vertexId,
                        executionId,
                        executionConfig,
                        taskInfo,
                        jobConfiguration,
                        taskConfiguration,
                        userCodeClassLoader,
                        memoryManager,
                        sharedResources,
                        ioManager,
                        broadcastVariableManager,
                        taskStateManager,
                        aggregateManager,
                        accumulatorRegistry,
                        kvStateRegistry,
                        inputSplitProvider,
                        distributedCacheEntries,
                        partitionWriters,
                        inputGates,
                        taskEventDispatcher,
                        checkpointResponder,
                        operatorCoordinatorEventGateway,
                        taskManagerConfig,
                        metrics,
                        this,
                        externalResourceInfoProvider,
                        channelStateExecutorFactory);

        // Make sure the user code classloader is accessible thread-locally.
        // We are setting the correct context class loader before instantiating the invokable
        // so that it is available to the invokable during its entire lifetime.
      // 将当前ExecutingThread的ContextClassLoader设定为userCodeClassLoader，以确保能够正常访问用户代码
        executingThread.setContextClassLoader(userCodeClassLoader.asClassLoader());

        // When constructing invokable, separate threads can be constructed and thus should be
        // monitored for system exit (in addition to invoking thread itself monitored below).
        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        try {
            // now load and instantiate the task's invokable code
          // 加载并初始化Task中的invokable代码，生成AbstractInvokable对象
            invokable =
                    loadAndInstantiateInvokable(
                            userCodeClassLoader.asClassLoader(), nameOfInvokableClass, env);
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
        }

        // ----------------------------------------------------------------
        //  actual task core work
        // ----------------------------------------------------------------

        // we must make strictly sure that the invokable is accessible to the cancel() call
        // by the time we switched to running.
      // 将invokable赋给成员变量，以便当Task状态转换为RUNNING时，能够在cancel()方法中获取invokable对象
        this.invokable = invokable;

        restoreAndInvoke(invokable);

        // make sure, we enter the catch block if the task leaves the invoke() method due
        // to the fact that it has been canceled
        if (isCanceledOrFailed()) {
            throw new CancelTaskException();
        }

        // ----------------------------------------------------------------
        //  finalization of a successful execution
        // ----------------------------------------------------------------

        // finish the produced partitions. if this fails, we consider the execution failed.
        for (ResultPartitionWriter partitionWriter : partitionWriters) {
            if (partitionWriter != null) {
                partitionWriter.finish();
            }
        }

        // try to mark the task as finished
        // if that fails, the task was canceled/failed in the meantime
        if (!transitionState(ExecutionState.RUNNING, ExecutionState.FINISHED)) {
            throw new CancelTaskException();
        }
    } catch (Throwable t) {
        // ----------------------------------------------------------------
        // the execution failed. either the invokable code properly failed, or
        // an exception was thrown as a side effect of cancelling
        // ----------------------------------------------------------------

        t = preProcessException(t);

        try {
            // transition into our final state. we should be either in DEPLOYING, INITIALIZING,
            // RUNNING, CANCELING, or FAILED
            // loop for multiple retries during concurrent state changes via calls to cancel()
            // or to failExternally()
            while (true) {
                ExecutionState current = this.executionState;

                if (current == ExecutionState.RUNNING
                        || current == ExecutionState.INITIALIZING
                        || current == ExecutionState.DEPLOYING) {
                    if (ExceptionUtils.findThrowable(t, CancelTaskException.class)
                            .isPresent()) {
                        if (transitionState(current, ExecutionState.CANCELED, t)) {
                            cancelInvokable(invokable);
                            break;
                        }
                    } else {
                        if (transitionState(current, ExecutionState.FAILED, t)) {
                            cancelInvokable(invokable);
                            break;
                        }
                    }
                } else if (current == ExecutionState.CANCELING) {
                    if (transitionState(current, ExecutionState.CANCELED)) {
                        break;
                    }
                } else if (current == ExecutionState.FAILED) {
                    // in state failed already, no transition necessary any more
                    break;
                }
                // unexpected state, go to failed
                else if (transitionState(current, ExecutionState.FAILED, t)) {
                    LOG.error(
                            "Unexpected state in task {} ({}) during an exception: {}.",
                            taskNameWithSubtask,
                            executionId,
                            current);
                    break;
                }
                // else fall through the loop and
            }
        } catch (Throwable tt) {
            String message =
                    String.format(
                            "FATAL - exception in exception handler of task %s (%s).",
                            taskNameWithSubtask, executionId);
            LOG.error(message, tt);
            notifyFatalError(message, tt);
        }
    } finally {
        try {
            LOG.info("Freeing task resources for {} ({}).", taskNameWithSubtask, executionId);

            // clear the reference to the invokable. this helps guard against holding references
            // to the invokable and its structures in cases where this Task object is still
            // referenced
            this.invokable = null;

            // free the network resources
            releaseResources();

            // free memory resources
            if (invokable != null) {
                memoryManager.releaseAll(invokable);
            }

            // remove all of the tasks resources
            fileCache.releaseJob(jobId, executionId);

            // close and de-activate safety net for task thread
            LOG.debug("Ensuring all FileSystem streams are closed for task {}", this);
            FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();

            notifyFinalState();
        } catch (Throwable t) {
            // an error in the resource cleanup is fatal
            String message =
                    String.format(
                            "FATAL - exception in resource cleanup of task %s (%s).",
                            taskNameWithSubtask, executionId);
            LOG.error(message, t);
            notifyFatalError(message, t);
        }

        // un-register the metrics at the end so that the task may already be
        // counted as finished when this happens
        // errors here will only be logged
        try {
            metrics.close();
        } catch (Throwable t) {
            LOG.error(
                    "Error during metrics de-registration of task {} ({}).",
                    taskNameWithSubtask,
                    executionId,
                    t);
        }
    }
}
```

Task实例实际上是启 动Operator实例的执行器，具体的计算逻辑还是在Operator中定义和 实现，接下来我们看看Task线程是如何触发Operator执行的  restoreAndInvoke(invokable);

```java
private void restoreAndInvoke(TaskInvokable finalInvokable) throws Exception {
    try {
        // switch to the INITIALIZING state, if that fails, we have been canceled/failed in the
        // meantime
      // 将当前的Task执行状态从DEPLOYING转换为RUNNING，如果转换失败，则抛出异常
        if (!transitionState(ExecutionState.DEPLOYING, ExecutionState.INITIALIZING)) {
            throw new CancelTaskException();
        }
// 将Task ExecutionState状态转换为RUNNING的消息通过taskManagerActions通知给其他 模块
        taskManagerActions.updateTaskExecutionState(
                new TaskExecutionState(executionId, ExecutionState.INITIALIZING));

        // make sure the user code classloader is accessible thread-locally
      // 再次确认userCodeClassLoader已经设定为本地线程ContextClassLoader
        executingThread.setContextClassLoader(userCodeClassLoader.asClassLoader());
// 执行invokable中的restore()方法
        runWithSystemExitMonitoring(finalInvokable::restore);

        if (!transitionState(ExecutionState.INITIALIZING, ExecutionState.RUNNING)) {
            throw new CancelTaskException();
        }

        // notify everyone that we switched to running
        taskManagerActions.updateTaskExecutionState(
                new TaskExecutionState(executionId, ExecutionState.RUNNING));
// 执行invokable中的invoke()方法，从而执行逻辑Task中的计算任务
        runWithSystemExitMonitoring(finalInvokable::invoke);
    } catch (Throwable throwable) {
        try {
            runWithSystemExitMonitoring(() -> finalInvokable.cleanUp(throwable));
        } catch (Throwable cleanUpThrowable) {
            throwable.addSuppressed(cleanUpThrowable);
        }
        throw throwable;
    }
    runWithSystemExitMonitoring(() -> finalInvokable.cleanUp(null));
}
```

