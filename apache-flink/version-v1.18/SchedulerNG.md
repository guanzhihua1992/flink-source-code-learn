SchedulerNG作为ExecutionGraph任务调度器接口，主要有 LegacyScheduler和DefaultScheduler两种实现类，两者最主要的区别 在于，LegacyScheduler会将Task的调度操作在ExecutionGraph内部进 行，而DefaultScheduler将调度的操作独立出来，在调度器中独立完 成。目前Flink默认的调度器为DefaultScheduler，主要因为 DefaultScheduler可以将调度的过程单独抽取出来，让 ExecutionGraph更加关注计算逻辑的定义

通过SchedulerNGFactory创建SchedulerNG，SchedulerNGFactory 有两种实现方式，分别为LegacySchedulerFactory和 DefaultSchedulerFactory，其中LegacySchedulerFactory用于创建 LegacyScheduler，DefaultSchedulerFactory用于创建 DefaultScheduler。

·同时SchedulerNG的默认基本实现类为SchedulerBase，最终实现 类为DefaultScheduler和LegacyScheduler。

·其中DefaultScheduler实现了SchedulerOperations接口，提供了 allocateSlotsAnddeploy()等方法，用于分配Slot资源及执行Execution。

·DefaultScheduler主要包含三个主要组件:ExecutionSlotAllocator、 ExecutionVertexOperations以及SchedulingStrategy接口实现类。其中 ExecutionSlotAllocator负责将Slot资源分配给Execution， ExecutionVertexOperations主要提供了ExecutionVertex执行的操作。

·SchedulingStrategy提供了两种默认实现方式，分别为 EagerSchedulingStrategy和LazyFromSourcesSchedulingStrategy。 EagerSchedulingStrategy将ExecutionJobVertex中所有的Task实例立即拉起 执行，适用于Streaming类型作业，LazyFromSourcesSchedulingStrategy需 要等待所有ExecutionJobVertex节点输入的数据到达后才开始分配资源 并计算，适用于批量计算模式。

1.SchedulerNG调度器的创建与启动 JobMaster.createScheduler 调用 slotPoolServiceSchedulerFactory.createScheduler

```java
JobMaster.createScheduler
slotPoolServiceSchedulerFactory.createScheduler
schedulerNGFactory.createInstance
```

schedulerNGFactory三个实现类DefaultSchedulerFactory AdaptiveSchedulerFactory AdaptiveBatchSchedulerFactory对应生成不同的 SchedulerNG,看下默认实现new DefaultScheduler()

启动 JobMaster.startScheduling

```java
private void startScheduling() {
    schedulerNG.startScheduling();
}
```

schedulerNG.startScheduling();

```java
@Override
public final void startScheduling() {
    mainThreadExecutor.assertRunningInMainThread();
    registerJobMetrics(
            jobManagerJobMetricGroup,
            executionGraph,
            this::getNumberOfRestarts,
            deploymentStateTimeMetrics,
            executionGraph::registerJobStatusListener,
            executionGraph.getStatusTimestamp(JobStatus.INITIALIZING),
            jobStatusMetricsSettings);
    operatorCoordinatorHandler.startAllOperatorCoordinators();
    startSchedulingInternal();
}
```

startSchedulingInternal();看下默认实现

```java
@Override
protected void startSchedulingInternal() {
    log.info(
            "Starting scheduling with scheduling strategy [{}]",
            schedulingStrategy.getClass().getName());
    transitionToRunning();
    schedulingStrategy.startScheduling();
}
```

transitionToRunning();

```java
protected final void transitionToRunning() {
    executionGraph.transitionToRunning();
}
```

executionGraph.transitionToRunning();

```java
@Override
public void transitionToRunning() {
    if (!transitionState(JobStatus.CREATED, JobStatus.RUNNING)) {
        throw new IllegalStateException(
                "Job may only be scheduled from state " + JobStatus.CREATED);
    }
}
```

transitionState

```java
private boolean transitionState(JobStatus current, JobStatus newState, Throwable error) {
    assertRunningInJobMasterMainThread();//确认主线程
    // consistency check
    if (current.isTerminalState()) {
        String message = "Job is trying to leave terminal state " + current;
        LOG.error(message);
        throw new IllegalStateException(message);
    }

    // now do the actual state transition
    if (state == current) {
        state = newState;
        LOG.info(
                "Job {} ({}) switched from state {} to {}.",
                getJobName(),
                getJobID(),
                current,
                newState,
                error);

        stateTimestamps[newState.ordinal()] = System.currentTimeMillis();
        notifyJobStatusChange(newState);//通知 jobStatusListeners
        notifyJobStatusHooks(newState, error);//通知 jobStatusHooks
        return true;
    } else {
        return false;
    }
}
```

schedulingStrategy.startScheduling();  SchedulingStrategy实现类有VertexwiseSchedulingStrategy和PipelinedRegionSchedulingStrategy

```java
@Override
public void startScheduling() {
    final Set<SchedulingPipelinedRegion> sourceRegions =
            IterableUtils.toStream(schedulingTopology.getAllPipelinedRegions())//拿到getAllPipelinedRegions
                    .filter(this::isSourceRegion)//过滤isSourceRegion
                    .collect(Collectors.toSet());
    maybeScheduleRegions(sourceRegions);
}
```

maybeScheduleRegions(sourceRegions);

```java
private void maybeScheduleRegions(final Set<SchedulingPipelinedRegion> regions) {
    final Set<SchedulingPipelinedRegion> regionsToSchedule = new HashSet<>();
    Set<SchedulingPipelinedRegion> nextRegions = regions;
    while (!nextRegions.isEmpty()) {
        nextRegions = addSchedulableAndGetNextRegions(nextRegions, regionsToSchedule);
    }
    // schedule regions in topological order.
    SchedulingStrategyUtils.sortPipelinedRegionsInTopologicalOrder(
                    schedulingTopology, regionsToSchedule)
            .forEach(this::scheduleRegion);
}
```

scheduleRegion

```java
private void scheduleRegion(final SchedulingPipelinedRegion region) {
    checkState(
            areRegionVerticesAllInCreatedState(region),
            "BUG: trying to schedule a region which is not in CREATED state");
    scheduledRegions.add(region);
    schedulerOperations.allocateSlotsAndDeploy(regionVerticesSorted.get(region));
}
```

schedulerOperations.allocateSlotsAndDeploy()

```java
@Override
public void allocateSlotsAndDeploy(final List<ExecutionVertexID> verticesToDeploy) {
    final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex =
            executionVertexVersioner.recordVertexModifications(verticesToDeploy);

    final List<Execution> executionsToDeploy =
            verticesToDeploy.stream()
                    .map(this::getCurrentExecutionOfVertex)
                    .collect(Collectors.toList());

    executionDeployer.allocateSlotsAndDeploy(executionsToDeploy, requiredVersionByVertex);
}
```

DefaultExecutionDeployer.allocateSlotsAndDeploy(executionsToDeploy, requiredVersionByVertex);

```java
@Override
public void allocateSlotsAndDeploy(
        final List<Execution> executionsToDeploy,
        final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex) {
    validateExecutionStates(executionsToDeploy);//状态判断

    transitionToScheduled(executionsToDeploy);//状态转换及通知

    final List<ExecutionSlotAssignment> executionSlotAssignments =
            allocateSlotsFor(executionsToDeploy);//slot 资源

    final List<ExecutionDeploymentHandle> deploymentHandles =
            createDeploymentHandles(
                    executionsToDeploy, requiredVersionByVertex, executionSlotAssignments);

    waitForAllSlotsAndDeploy(deploymentHandles);
}
```

waitForAllSlotsAndDeploy(deploymentHandles);

```java
private void waitForAllSlotsAndDeploy(final List<ExecutionDeploymentHandle> deploymentHandles) {
    FutureUtils.assertNoException(
            assignAllResourcesAndRegisterProducedPartitions(deploymentHandles)
                    .handle(deployAll(deploymentHandles)));
}
```

deployAll(deploymentHandles)

```java
private BiFunction<Void, Throwable, Void> deployAll(
        final List<ExecutionDeploymentHandle> deploymentHandles) {
    return (ignored, throwable) -> {
        propagateIfNonNull(throwable);
        for (final ExecutionDeploymentHandle deploymentHandle : deploymentHandles) {
            final CompletableFuture<LogicalSlot> slotAssigned =
                    deploymentHandle.getLogicalSlotFuture();
            checkState(slotAssigned.isDone());

            FutureUtils.assertNoException(
                    slotAssigned.handle(deployOrHandleError(deploymentHandle)));
        }
        return null;
    };
}
```

deployOrHandleError

```java
private BiFunction<Object, Throwable, Void> deployOrHandleError(
        final ExecutionDeploymentHandle deploymentHandle) {

    return (ignored, throwable) -> {
        final ExecutionVertexVersion requiredVertexVersion =
                deploymentHandle.getRequiredVertexVersion();
        final Execution execution = deploymentHandle.getExecution();

        if (execution.getState() != ExecutionState.SCHEDULED
                || executionVertexVersioner.isModified(requiredVertexVersion)) {
            if (throwable == null) {
                log.debug(
                        "Refusing to assign slot to execution {} because this deployment was "
                                + "superseded by another deployment",
                        deploymentHandle.getExecutionAttemptId());
            }
            return null;
        }

        if (throwable == null) {
            deployTaskSafe(execution);
        } else {
            handleTaskDeploymentFailure(execution, throwable);
        }
        return null;
    };
}
```

deployTaskSafe(execution);

```java
private void deployTaskSafe(final Execution execution) {
    try {
        executionOperations.deploy(execution);
    } catch (Throwable e) {
        handleTaskDeploymentFailure(execution, e);
    }
}
```

DefaultExecutionOperations.deploy(execution)

```java
@Override
public void deploy(Execution execution) throws JobException {
  // 调用Execution中的deploy()方法，执行当前节点的Execution
    execution.deploy();
}
```

execution.deploy();

```java
public void deploy() throws JobException {
  // 确认JobMasterMainThread状态为Running
    assertRunningInJobMasterMainThread();
// 获取已经分配的Slot资源
    final LogicalSlot slot = assignedResource;

    checkNotNull(
            slot,
            "In order to deploy the execution we first have to assign a resource via tryAssignResource.");

    // Check if the TaskManager died in the meantime
    // This only speeds up the response to TaskManagers failing concurrently to deployments.
    // The more general check is the rpcTimeout of the deployment call
  // 检查并判断当前的Slot对应的TaskManager是否存活，如果该Slot分配的TaskManager不存活，则抛出异常
    if (!slot.isAlive()) {
        throw new JobException("Target slot (TaskManager) for deployment is no longer alive.");
    }

    // make sure exactly one deployment call happens from the correct state
  // 判断当前的ExecutionState是否为SCHEDULED或者CREATED，如果不是则抛出异常
    ExecutionState previous = this.state;
    if (previous == SCHEDULED) {
        if (!transitionState(previous, DEPLOYING)) {
            // race condition, someone else beat us to the deploying call.
            // this should actually not happen and indicates a race somewhere else
          // 表示当前的Execution已经被取消或已经执行
            throw new IllegalStateException(
                    "Cannot deploy task: Concurrent deployment call race.");
        }
    } else {
        // vertex may have been cancelled, or it was already scheduled
        throw new IllegalStateException(
                "The vertex must be in SCHEDULED state to be deployed. Found state "
                        + previous);
    }
// 判断当前Execution分配的Slot资源的有效性
    if (this != slot.getPayload()) {
        throw new IllegalStateException(
                String.format(
                        "The execution %s has not been assigned to the assigned slot.", this));
    }

    try {

        // race double check, did we fail/cancel and do we need to release the slot?
      // 检查当前Execution的状态，如果状态不是DEPLOYING，则释放资源
        if (this.state != DEPLOYING) {
            slot.releaseSlot(
                    new FlinkException(
                            "Actual state of execution "
                                    + this
                                    + " ("
                                    + state
                                    + ") does not match expected state DEPLOYING."));
            return;
        }

        LOG.info(
                "Deploying {} (attempt #{}) with attempt id {} and vertex id {} to {} with allocation id {}",
                vertex.getTaskNameWithSubtaskIndex(),
                getAttemptNumber(),
                attemptId,
                vertex.getID(),
                getAssignedResourceLocation(),
                slot.getAllocationId());
//创建TaskDeploymentDescriptor，用于将Task部署到TaskManager
        final TaskDeploymentDescriptor deployment =
                TaskDeploymentDescriptorFactory.fromExecution(this)
                        .createDeploymentDescriptor(
                                slot.getAllocationId(),
                                taskRestore,
                                producedPartitions.values());

        // null taskRestore to let it be GC'ed
      // 将taskRestore置空，以进行GC回收
        taskRestore = null;
//获取TaskManagerGateWay
        final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();
// 获取jobMasterMainThreadExecutor运行线程池
        final ComponentMainThreadExecutor jobMasterMainThreadExecutor =
                vertex.getExecutionGraphAccessor().getJobMasterMainThreadExecutor();

        getVertex().notifyPendingDeployment(this);
        // We run the submission in the future executor so that the serialization of large TDDs
        // does not block
        // the main thread and sync back to the main thread once submission is completed.
      // 调用taskManagerGateway.submit()方法提交Deployment对象到TaskExecutor
        CompletableFuture.supplyAsync(
                        () -> taskManagerGateway.submitTask(deployment, rpcTimeout), executor)
                .thenCompose(Function.identity())
                .whenCompleteAsync(
                        (ack, failure) -> {
                            if (failure == null) {
                                vertex.notifyCompletedDeployment(this);
                            } else {// 如果出现异常，则调用markFailed()方法更新Task运行状态
                                final Throwable actualFailure =
                                        ExceptionUtils.stripCompletionException(failure);

                                if (actualFailure instanceof TimeoutException) {
                                    String taskname =
                                            vertex.getTaskNameWithSubtaskIndex()
                                                    + " ("
                                                    + attemptId
                                                    + ')';

                                    markFailed(
                                            new Exception(
                                                    "Cannot deploy task "
                                                            + taskname
                                                            + " - TaskManager ("
                                                            + getAssignedResourceLocation()
                                                            + ") not responding after a rpcTimeout of "
                                                            + rpcTimeout,
                                                    actualFailure));
                                } else {
                                    markFailed(actualFailure);
                                }
                            }
                        },
                        jobMasterMainThreadExecutor);

    } catch (Throwable t) {
        markFailed(t);
    }
}
```

完成上述步骤后，TaskManager接收到JobManager提交的 TaskDeploymentDescriptor信息，完成Task线程的构建并启动运行。 当Job所有的Task实例全部运行后，系统就可以正常处理接入的数据 了。

启新线程taskManagerGateway.submitTask(deployment, rpcTimeout)

```java
@Override
public CompletableFuture<Acknowledge> submitTask(TaskDeploymentDescriptor tdd, Time timeout) {
    return taskExecutorGateway.submitTask(tdd, jobMasterId, timeout);
}
```