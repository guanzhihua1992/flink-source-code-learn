TaskExecutor启动后，会立即向ResourceManager中注册当前 TaskManager的信息.JobMaster组件启动后也立即会向 ResourceManager注册JobMaster的信息.集群组件之间的RPC通信都会通过创建 RegisteredRpcConnection进行.RegisteredRpcConnection实现子类主要有 JobManagerRegisteredRpcConnection、ResourceManagerConnection 和TaskExecutorToResourceManagerConnection三种.

·JobManagerRegisteredRpcConnection:用于管理TaskManager中与 JobManager之间的RPC连接。

·ResourceManagerConnection:用于管理JobManager中与 ResourceManager之间的RPC连接。

·TaskExecutorToResourceManagerConnection:用于管理 TaskExecutor中与ResourceManager之间的RPC连接。

以 TaskExecutor 启动为例

```java
@Override
public void onStart() throws Exception {
    try {
        startTaskExecutorServices();
    } catch (Throwable t) {
        final TaskManagerException exception =
                new TaskManagerException(
                        String.format("Could not start the TaskExecutor %s", getAddress()), t);
        onFatalError(exception);
        throw exception;
    }

    startRegistrationTimeout();
}
```

startTaskExecutorServices

```java
private void startTaskExecutorServices() throws Exception {
    try {
        // start by connecting to the ResourceManager
        resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());

        // tell the task slot table who's responsible for the task slot actions
        taskSlotTable.start(new SlotActionsImpl(), getMainThreadExecutor());

        // start the job leader service
        jobLeaderService.start(
                getAddress(), getRpcService(), haServices, new JobLeaderListenerImpl());

        fileCache =
                new FileCache(
                        taskManagerConfiguration.getTmpDirectories(),
                        taskExecutorBlobService.getPermanentBlobService());

        tryLoadLocalAllocationSnapshots();
    } catch (Exception e) {
        handleStartTaskExecutorServicesException(e);
    }
}
```

ResourceManagerLeaderListener 监听消息 接收到来自 ResourceManager的leaderAddress以及leaderSessionID的信息后

```java
@Override
public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
    runAsync(
            () ->
                    notifyOfNewResourceManagerLeader(
                            leaderAddress,
                            ResourceManagerId.fromUuidOrNull(leaderSessionID)));
}
```

```java
private void notifyOfNewResourceManagerLeader(
        String newLeaderAddress, ResourceManagerId newResourceManagerId) {
    resourceManagerAddress =
            createResourceManagerAddress(newLeaderAddress, newResourceManagerId);
    reconnectToResourceManager(
            new FlinkException(
                    String.format(
                            "ResourceManager leader changed to new address %s",
                            resourceManagerAddress)));
}
```

```java
private void reconnectToResourceManager(Exception cause) {
    closeResourceManagerConnection(cause);
    startRegistrationTimeout();
    tryConnectToResourceManager();
}
```

```java
private void tryConnectToResourceManager() {
    if (resourceManagerAddress != null) {
        connectToResourceManager();
    }
}
```

```java
private void connectToResourceManager() {
    assert (resourceManagerAddress != null);
    assert (establishedResourceManagerConnection == null);
    assert (resourceManagerConnection == null);

    log.info("Connecting to ResourceManager {}.", resourceManagerAddress);

    final TaskExecutorRegistration taskExecutorRegistration =
            new TaskExecutorRegistration(
                    getAddress(),
                    getResourceID(),
                    unresolvedTaskManagerLocation.getDataPort(),
                    JMXService.getPort().orElse(-1),
                    hardwareDescription,
                    memoryConfiguration,
                    taskManagerConfiguration.getDefaultSlotResourceProfile(),
                    taskManagerConfiguration.getTotalResourceProfile(),
                    unresolvedTaskManagerLocation.getNodeId());

    resourceManagerConnection =
            new TaskExecutorToResourceManagerConnection(
                    log,
                    getRpcService(),
                    taskManagerConfiguration.getRetryingRegistrationConfiguration(),
                    resourceManagerAddress.getAddress(),
                    resourceManagerAddress.getResourceManagerId(),
                    getMainThreadExecutor(),
                    new ResourceManagerRegistrationListener(),
                    taskExecutorRegistration);
    resourceManagerConnection.start();
}
```

完成重新连接