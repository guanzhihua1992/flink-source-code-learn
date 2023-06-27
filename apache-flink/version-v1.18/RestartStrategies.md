Task重启策略

RestartStrategies返回几种类型的重启策略配置

```java
/**
 * This class defines methods to generate RestartStrategyConfigurations. These configurations are
 * used to create RestartStrategies at runtime.
 *
 * <p>The RestartStrategyConfigurations are used to decouple the core module from the runtime
 * module.
 */
@PublicEvolving
public class RestartStrategies {

    /**
     * Generates NoRestartStrategyConfiguration.
     *
     * @return NoRestartStrategyConfiguration
     */
    public static RestartStrategyConfiguration noRestart() {
        return new NoRestartStrategyConfiguration();
    }

    public static RestartStrategyConfiguration fallBackRestart() {
        return new FallbackRestartStrategyConfiguration();
    }

    /**
     * Generates a FixedDelayRestartStrategyConfiguration.
     *
     * @param restartAttempts Number of restart attempts for the FixedDelayRestartStrategy
     * @param delayBetweenAttempts Delay in-between restart attempts for the
     *     FixedDelayRestartStrategy
     * @return FixedDelayRestartStrategy
     */
    public static RestartStrategyConfiguration fixedDelayRestart(
            int restartAttempts, long delayBetweenAttempts) {
        return fixedDelayRestart(
                restartAttempts, Time.of(delayBetweenAttempts, TimeUnit.MILLISECONDS));
    }

    /**
     * Generates a FixedDelayRestartStrategyConfiguration.
     *
     * @param restartAttempts Number of restart attempts for the FixedDelayRestartStrategy
     * @param delayInterval Delay in-between restart attempts for the FixedDelayRestartStrategy
     * @return FixedDelayRestartStrategy
     */
    public static RestartStrategyConfiguration fixedDelayRestart(
            int restartAttempts, Time delayInterval) {
        return new FixedDelayRestartStrategyConfiguration(restartAttempts, delayInterval);
    }

    /**
     * Generates a FailureRateRestartStrategyConfiguration.
     *
     * @param failureRate Maximum number of restarts in given interval {@code failureInterval}
     *     before failing a job
     * @param failureInterval Time interval for failures
     * @param delayInterval Delay in-between restart attempts
     */
    public static FailureRateRestartStrategyConfiguration failureRateRestart(
            int failureRate, Time failureInterval, Time delayInterval) {
        return new FailureRateRestartStrategyConfiguration(
                failureRate, failureInterval, delayInterval);
    }

    /**
     * Generates a ExponentialDelayRestartStrategyConfiguration.
     *
     * @param initialBackoff Starting duration between restarts
     * @param maxBackoff The highest possible duration between restarts
     * @param backoffMultiplier Delay multiplier how many times is the delay longer than before
     * @param resetBackoffThreshold How long the job must run smoothly to reset the time interval
     * @param jitterFactor How much the delay may differ (in percentage)
     */
    public static ExponentialDelayRestartStrategyConfiguration exponentialDelayRestart(
            Time initialBackoff,
            Time maxBackoff,
            double backoffMultiplier,
            Time resetBackoffThreshold,
            double jitterFactor) {
        return new ExponentialDelayRestartStrategyConfiguration(
                initialBackoff, maxBackoff, backoffMultiplier, resetBackoffThreshold, jitterFactor);
    }
```

RestartStrategyConfiguration重启策略abstract类实现类主要有

NoRestartStrategyConfiguration 无重启:如果Job出现意外停止，则直接重启失败不再重启。

FixedDelayRestartStrategyConfiguration 固定延时重启:按照restart-strategy.fixed-delay.delay参数给出的固 定间隔重启Job，如果重启次数达到fixed-delay.attempt配置值仍没有重启成功，则停止重启。

ExponentialDelayRestartStrategyConfiguration 间隔时间渐缩时间间隔重启

FailureRateRestartStrategyConfiguration 按失败率重启:按照restart-strategy.failure-rate.delay参数给出的固 定间隔重启Job，如果重启次数在failure-rate-interval参数规定的时间周期 内到达max-failures-per-interval配置的阈值仍没有成功，则停止重启。

FallbackRestartStrategyConfiguration cluster level restartstrategy 集群级别重启

看下配置参数解析

```java
private static RestartStrategyConfiguration parseConfiguration(
        String restartstrategyKind, ReadableConfig configuration) {
    switch (restartstrategyKind.toLowerCase()) {
        case "none":
        case "off":
        case "disable":
            return noRestart();
        case "fixeddelay":
        case "fixed-delay":
            int attempts =
                    configuration.get(
                            RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS);
            Duration delay =
                    configuration.get(
                            RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY);
            return fixedDelayRestart(attempts, delay.toMillis());
        case "exponentialdelay":
        case "exponential-delay":
            Duration initialBackoff =
                    configuration.get(
                            RestartStrategyOptions
                                    .RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF);
            Duration maxBackoff =
                    configuration.get(
                            RestartStrategyOptions
                                    .RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF);
            double backoffMultiplier =
                    configuration.get(
                            RestartStrategyOptions
                                    .RESTART_STRATEGY_EXPONENTIAL_DELAY_BACKOFF_MULTIPLIER);
            Duration resetBackoffThreshold =
                    configuration.get(
                            RestartStrategyOptions
                                    .RESTART_STRATEGY_EXPONENTIAL_DELAY_RESET_BACKOFF_THRESHOLD);
            double jitter =
                    configuration.get(
                            RestartStrategyOptions
                                    .RESTART_STRATEGY_EXPONENTIAL_DELAY_JITTER_FACTOR);
            return exponentialDelayRestart(
                    Time.milliseconds(initialBackoff.toMillis()),
                    Time.milliseconds(maxBackoff.toMillis()),
                    backoffMultiplier,
                    Time.milliseconds(resetBackoffThreshold.toMillis()),
                    jitter);
        case "failurerate":
        case "failure-rate":
            int maxFailures =
                    configuration.get(
                            RestartStrategyOptions
                                    .RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL);
            Duration failureRateInterval =
                    configuration.get(
                            RestartStrategyOptions
                                    .RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL);
            Duration failureRateDelay =
                    configuration.get(
                            RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY);
            return failureRateRestart(
                    maxFailures,
                    Time.milliseconds(failureRateInterval.toMillis()),
                    Time.milliseconds(failureRateDelay.toMillis()));
        default:
            throw new IllegalArgumentException(
                    "Unknown restart strategy " + restartstrategyKind + ".");
    }
}
```

重启策略起始点 SchedulerNG.createInstance

```java
final RestartBackoffTimeStrategy restartBackoffTimeStrategy =
        RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        jobGraph.getSerializedExecutionConfig()
                                .deserializeValue(userCodeLoader)
                                .getRestartStrategy(),
                        jobMasterConfiguration,
                        jobGraph.isCheckpointingEnabled())
                .create();
```

RestartBackoffTimeStrategyFactoryLoader /** A utility class to load {@link RestartBackoffTimeStrategy.Factory} from the configuration. */ 一个以configuration创建RestartBackoffTimeStrategy.Factory工具类 RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory

```java
/**
 * Creates {@link RestartBackoffTimeStrategy.Factory} from the given configuration.
 *
 * <p>The strategy factory is decided in order as follows:
 *
 * <ol>
 *   <li>Strategy set within job graph, i.e. {@link
 *       RestartStrategies.RestartStrategyConfiguration}, unless the config is {@link
 *       RestartStrategies.FallbackRestartStrategyConfiguration}.
 *   <li>Strategy set in the cluster(server-side) config (flink-conf.yaml), unless the strategy
 *       is not specified
 *   <li>{@link
 *       FixedDelayRestartBackoffTimeStrategy.FixedDelayRestartBackoffTimeStrategyFactory} if
 *       checkpointing is enabled. Otherwise {@link
 *       NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory}
 * </ol>
 *
 * @param jobRestartStrategyConfiguration restart configuration given within the job graph
 * @param clusterConfiguration cluster(server-side) configuration
 * @param isCheckpointingEnabled if checkpointing is enabled for the job
 * @return new version restart strategy factory
 */
//入参包括三个 job级别配置 cluster级别配置 是否支持Checkpointing
public static RestartBackoffTimeStrategy.Factory createRestartBackoffTimeStrategyFactory(
        final RestartStrategies.RestartStrategyConfiguration jobRestartStrategyConfiguration,
        final Configuration clusterConfiguration,
        final boolean isCheckpointingEnabled) {

    checkNotNull(jobRestartStrategyConfiguration);
    checkNotNull(clusterConfiguration);
//顺序 先job级别 然后cluster级别 最后才以默认策略
    return getJobRestartStrategyFactory(jobRestartStrategyConfiguration)
            .orElse(
                    getClusterRestartStrategyFactory(clusterConfiguration)
                            .orElse(getDefaultRestartStrategyFactory(isCheckpointingEnabled)));
}
```

getJobRestartStrategyFactory

```java
private static Optional<RestartBackoffTimeStrategy.Factory> getJobRestartStrategyFactory(
        final RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration) {
//无重启
    if (restartStrategyConfiguration instanceof NoRestartStrategyConfiguration) {
        return Optional.of(
                NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory.INSTANCE);
      //固定延时重启
    } else if (restartStrategyConfiguration instanceof FixedDelayRestartStrategyConfiguration) {
        final FixedDelayRestartStrategyConfiguration fixedDelayConfig =
                (FixedDelayRestartStrategyConfiguration) restartStrategyConfiguration;

        return Optional.of(
                new FixedDelayRestartBackoffTimeStrategy
                        .FixedDelayRestartBackoffTimeStrategyFactory(
                        fixedDelayConfig.getRestartAttempts(),
                        fixedDelayConfig.getDelayBetweenAttemptsInterval().toMilliseconds()));
      //失败率重启
    } else if (restartStrategyConfiguration
            instanceof FailureRateRestartStrategyConfiguration) {
        final FailureRateRestartStrategyConfiguration failureRateConfig =
                (FailureRateRestartStrategyConfiguration) restartStrategyConfiguration;

        return Optional.of(
                new FailureRateRestartBackoffTimeStrategy
                        .FailureRateRestartBackoffTimeStrategyFactory(
                        failureRateConfig.getMaxFailureRate(),
                        failureRateConfig.getFailureInterval().toMilliseconds(),
                        failureRateConfig.getDelayBetweenAttemptsInterval().toMilliseconds()));
      //return Optional.empty(); job级别不支持FallbackRestartStrategyConfiguration
    } else if (restartStrategyConfiguration instanceof FallbackRestartStrategyConfiguration) {
        return Optional.empty();
      //延时缩进重启
    } else if (restartStrategyConfiguration
            instanceof ExponentialDelayRestartStrategyConfiguration) {
        final ExponentialDelayRestartStrategyConfiguration exponentialDelayConfig =
                (ExponentialDelayRestartStrategyConfiguration) restartStrategyConfiguration;
        return Optional.of(
                new ExponentialDelayRestartBackoffTimeStrategy
                        .ExponentialDelayRestartBackoffTimeStrategyFactory(
                        exponentialDelayConfig.getInitialBackoff().toMilliseconds(),
                        exponentialDelayConfig.getMaxBackoff().toMilliseconds(),
                        exponentialDelayConfig.getBackoffMultiplier(),
                        exponentialDelayConfig.getResetBackoffThreshold().toMilliseconds(),
                        exponentialDelayConfig.getJitterFactor()));
    } else {
        throw new IllegalArgumentException(
                "Unknown restart strategy configuration " + restartStrategyConfiguration + ".");
    }
}
```

getClusterRestartStrategyFactory 

```java
private static Optional<RestartBackoffTimeStrategy.Factory> getClusterRestartStrategyFactory(
        final Configuration clusterConfiguration) {

    final String restartStrategyName =
            clusterConfiguration.getString(RestartStrategyOptions.RESTART_STRATEGY);
    if (restartStrategyName == null) {
        return Optional.empty();
    }

    switch (restartStrategyName.toLowerCase()) {
        case "none":
        case "off":
        case "disable":
            return Optional.of(
                    NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory.INSTANCE);
        case "fixeddelay":
        case "fixed-delay":
            return Optional.of(
                    FixedDelayRestartBackoffTimeStrategy.createFactory(clusterConfiguration));
        case "failurerate":
        case "failure-rate":
            return Optional.of(
                    FailureRateRestartBackoffTimeStrategy.createFactory(clusterConfiguration));
        case "exponentialdelay":
        case "exponential-delay":
            return Optional.of(
                    ExponentialDelayRestartBackoffTimeStrategy.createFactory(
                            clusterConfiguration));
        default:
            throw new IllegalArgumentException(
                    "Unknown restart strategy " + restartStrategyName + ".");
    }
}
```

getDefaultRestartStrategyFactory

```java
private static RestartBackoffTimeStrategy.Factory getDefaultRestartStrategyFactory(
        final boolean isCheckpointingEnabled) {
//支持Checkpointing FixedDelayRestartBackoffTimeStrategy 否则 NoRestartBackoffTimeStrategy
    if (isCheckpointingEnabled) {
        // fixed delay restart strategy with default params
        return new FixedDelayRestartBackoffTimeStrategy
                .FixedDelayRestartBackoffTimeStrategyFactory(
                DEFAULT_RESTART_ATTEMPTS, DEFAULT_RESTART_DELAY);
    } else {
        return NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory.INSTANCE;
    }
}
```

RestartBackoffTimeStrategy 决定是否重启以及重启时间的策略类

```
/** Strategy to decide whether to restart failed tasks and the delay to do the restarting. */
public interface RestartBackoffTimeStrategy {}
```

重启起始点 AdaptiveScheduler.goToRestarting

```java
@Override
public void goToRestarting(
        ExecutionGraph executionGraph,
        ExecutionGraphHandler executionGraphHandler,
        OperatorCoordinatorHandler operatorCoordinatorHandler,
        Duration backoffTime,
        List<ExceptionHistoryEntry> failureCollection) {

    for (ExecutionVertex executionVertex : executionGraph.getAllExecutionVertices()) {
        final int attemptNumber =
                executionVertex.getCurrentExecutionAttempt().getAttemptNumber();

        this.vertexAttemptNumberStore.setAttemptCount(
                executionVertex.getJobvertexId(),
                executionVertex.getParallelSubtaskIndex(),
                attemptNumber + 1);
    }

    transitionToState(
            new Restarting.Factory(
                    this,
                    executionGraph,
                    executionGraphHandler,
                    operatorCoordinatorHandler,
                    LOG,
                    backoffTime,
                    userCodeClassLoader,
                    failureCollection));
    numRestarts++;
}
```