YarnSessionClusterEntrypoint包含JVM进程启动的main()入口方法， 最终通过在启动脚本中运行YarnSessionClusterEntrypoint的main() 方法，实现YarnSessionCluster集群的创建和启动。

```java
public static void main(String[] args) {
    // startup checks and logging
  //启动前检查并设定参数，如日志的配置，然后调用 JvmShutdownSafeguard.installAsShutdownHook(LOG)向JVM中安装 safeguard shutdown hook，保障集群异常停止过程中有足够的时间处 理线程中的数据。
    EnvironmentInformation.logEnvironmentInfo(
            LOG, YarnSessionClusterEntrypoint.class.getSimpleName(), args);
    SignalHandler.register(LOG);
    JvmShutdownSafeguard.installAsShutdownHook(LOG);
// 获取系统环境变量
    Map<String, String> env = System.getenv();
// 获取工作路径
    final String workingDirectory = env.get(ApplicationConstants.Environment.PWD.key());
    Preconditions.checkArgument(
            workingDirectory != null,
            "Working directory variable (%s) not set",
            ApplicationConstants.Environment.PWD.key());

    try {// 加载Yarn需要的环境信息
        YarnEntrypointUtils.logYarnEnvironmentInformation(env, LOG);
    } catch (IOException e) {
        LOG.warn("Could not log YARN environment information.", e);
    }
// 创建加载Configuration配置
    final Configuration dynamicParameters =
            ClusterEntrypointUtils.parseParametersOrExit(
                    args,
                    new DynamicParametersConfigurationParserFactory(),
                    YarnSessionClusterEntrypoint.class);
    final Configuration configuration =
            YarnEntrypointUtils.loadConfiguration(workingDirectory, dynamicParameters, env);
// 创建YarnSessionClusterEntrypoint对象
    YarnSessionClusterEntrypoint yarnSessionClusterEntrypoint =
            new YarnSessionClusterEntrypoint(configuration);
// 通过ClusterEntrypoint执行yarnSessionClusterEntrypoint
    ClusterEntrypoint.runClusterEntrypoint(yarnSessionClusterEntrypoint);
}
```

JvmShutdownSafeguard.installAsShutdownHook(LOG);

```java
/**
 * Installs the safeguard shutdown hook. The maximum time that the JVM is allowed to spend on
 * shutdown before being killed is five seconds.
 *
 * @param logger The logger to log errors to.
 */
public static void installAsShutdownHook(Logger logger) {
    installAsShutdownHook(logger, DEFAULT_DELAY);
}
```

```java
/**
 * Installs the safeguard shutdown hook. The maximum time that the JVM is allowed to spend on
 * shutdown before being killed is the given number of milliseconds.
 *
 * @param logger The logger to log errors to.
 * @param delayMillis The delay (in milliseconds) to wait after clean shutdown was stared,
 *     before forcibly terminating the JVM.
 */
public static void installAsShutdownHook(Logger logger, long delayMillis) {
    checkArgument(delayMillis >= 0, "delay must be >= 0");

    // install the blocking shutdown hook
    Thread shutdownHook = new JvmShutdownSafeguard(delayMillis);
    ShutdownHookUtil.addShutdownHookThread(
            shutdownHook, JvmShutdownSafeguard.class.getSimpleName(), logger);
}
```

在YarnSessionClusterEntrypoint中实现了 createDispatcherResourceManagerComponentFactory()方法，在该方 法中将YarnResourceManagerFactory实例作为参数传递给 DispatcherResourceManagerComponentFactory，然后通过 DispatcherResourceManagerComponentFactory创建Yarn Session集群 中的组件和服务。在YarnSessionClusterEntrypoint的启动过程中获取YarnResourceManager的工厂类，通过YarnResourceManagerFactory 创建YarnResourceManager实例。

YarnSessionClusterEntrypoint.createDispatcherResourceManagerComponentFactory

```java
@Override
protected DispatcherResourceManagerComponentFactory
        createDispatcherResourceManagerComponentFactory(Configuration configuration) {
    return DefaultDispatcherResourceManagerComponentFactory.createSessionComponentFactory(
            YarnResourceManagerFactory.getInstance());
}
```

DefaultDispatcherResourceManagerComponentFactory.createSessionComponentFactory

```java
public static DefaultDispatcherResourceManagerComponentFactory createSessionComponentFactory(
        ResourceManagerFactory<?> resourceManagerFactory) {
    return new DefaultDispatcherResourceManagerComponentFactory(
            DefaultDispatcherRunnerFactory.createSessionRunner(
                    SessionDispatcherFactory.INSTANCE),
            resourceManagerFactory,
            SessionRestEndpointFactory.INSTANCE);
}
```