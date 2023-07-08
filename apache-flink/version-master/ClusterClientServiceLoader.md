DefaultClusterClientServiceLoader类 中创建了ServiceLoader的静态变量，其中ServiceLoader会根据配置 信息加载符合条件的ClusterClientFactory实现类。当加载的工厂类 不为空时，会调用factory.isCompatibleWith(configuration)判断该 工厂类是否符合作业提交时指定的目标集群类型。

ClusterClientServiceLoader接口类 目标是判断Configuration获取ClusterClientFactory

```java
/**
 * An interface used to discover the appropriate {@link ClusterClientFactory cluster client factory}
 * based on the provided {@link Configuration}.
 */
public interface ClusterClientServiceLoader {

    /**
     * Discovers the appropriate {@link ClusterClientFactory} based on the provided configuration.
     *
     * @param configuration the configuration based on which the appropriate factory is going to be
     *     used.
     * @return the appropriate {@link ClusterClientFactory}.
     */
    <ClusterID> ClusterClientFactory<ClusterID> getClusterClientFactory(
            final Configuration configuration);

    /**
     * Loads and returns a stream of the names of all available execution target names for {@code
     * Application Mode}.
     */
    Stream<String> getApplicationModeTargetNames();
}
```

ClusterClientServiceLoader 默认实现类 DefaultClusterClientServiceLoader

```java
/** A service provider for {@link ClusterClientFactory cluster client factories}. */
@Internal
public class DefaultClusterClientServiceLoader implements ClusterClientServiceLoader {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultClusterClientServiceLoader.class);

    @Override
    public <ClusterID> ClusterClientFactory<ClusterID> getClusterClientFactory(
            final Configuration configuration) {
        checkNotNull(configuration);
// 通过Java Service Loader加载配置的ClusterClientFactory对象
        final ServiceLoader<ClusterClientFactory> loader =
                ServiceLoader.load(ClusterClientFactory.class);
// 创建ClusterClientFactory集合
        final List<ClusterClientFactory> compatibleFactories = new ArrayList<>();
        final Iterator<ClusterClientFactory> factories = loader.iterator();
        while (factories.hasNext()) {
            try {
                final ClusterClientFactory factory = factories.next();
              // 将符合条件的ClusterClientFactory放置到集合中
                if (factory != null && factory.isCompatibleWith(configuration)) {
                    compatibleFactories.add(factory);
                }
            } catch (Throwable e) {
                if (e.getCause() instanceof NoClassDefFoundError) {
                    LOG.info("Could not load factory due to missing dependencies.");
                } else {
                    throw e;
                }
            }
        }
// 仅获取一个符合条件的ClusterClientFactory，如果获取到多个则抛出异常
        if (compatibleFactories.size() > 1) {
            final List<String> configStr =
                    configuration.toMap().entrySet().stream()
                            .map(e -> e.getKey() + "=" + e.getValue())
                            .collect(Collectors.toList());

            throw new IllegalStateException(
                    "Multiple compatible client factories found for:\n"
                            + String.join("\n", configStr)
                            + ".");
        }
// 仅获取一个符合条件的ClusterClientFactory，如果为空抛出异常
        if (compatibleFactories.isEmpty()) {
            throw new IllegalStateException(
                    "No ClusterClientFactory found. If you were targeting a Yarn cluster, "
                            + "please make sure to export the HADOOP_CLASSPATH environment variable or have hadoop in your "
                            + "classpath. For more information refer to the \"Deployment\" section of the official "
                            + "Apache Flink documentation.");
        }
// 返回通过ServiceLoader加载进来的ClusterClientFactory
        return (ClusterClientFactory<ClusterID>) compatibleFactories.get(0);
    }

    @Override
    public Stream<String> getApplicationModeTargetNames() {
        final ServiceLoader<ClusterClientFactory> loader =
                ServiceLoader.load(ClusterClientFactory.class);

        final List<String> result = new ArrayList<>();

        final Iterator<ClusterClientFactory> it = loader.iterator();
        while (it.hasNext()) {
            try {
                final ClusterClientFactory clientFactory = it.next();

                final Optional<String> applicationName = clientFactory.getApplicationTargetName();
                if (applicationName.isPresent()) {
                    result.add(applicationName.get());
                }

            } catch (ServiceConfigurationError e) {
                // cannot be loaded, most likely because Hadoop is not
                // in the classpath, we can ignore it for now.
            }
        }
        return result.stream();
    }
}
```

判断逻辑factory.isCompatibleWith(configuration),ClusterClientFactory有一个抽象实现类AbstractContainerizedClusterClientFactory,AbstractContainerizedClusterClientFactory有三个实现类 StandaloneClientFactory YarnClusterClientFactory KubernetesClusterClientFactory

```
/** A factory containing all the necessary information for creating clients to Flink clusters. */
@Internal
public interface ClusterClientFactory<ClusterID> {

    /**
     * Returns {@code true} if the current {@link ClusterClientFactory} is compatible with the
     * provided configuration, {@code false} otherwise.
     */
    boolean isCompatibleWith(Configuration configuration);

    /**
     * Create a {@link ClusterDescriptor} from the given configuration.
     *
     * @param configuration containing the configuration options relevant for the {@link
     *     ClusterDescriptor}
     * @return the corresponding {@link ClusterDescriptor}.
     */
    ClusterDescriptor<ClusterID> createClusterDescriptor(Configuration configuration);

    /**
     * Returns the cluster id if a cluster id is specified in the provided configuration, otherwise
     * it returns {@code null}.
     *
     * <p>A cluster id identifies a running cluster, e.g. the Yarn application id for a Flink
     * cluster running on Yarn.
     *
     * @param configuration containing the configuration options relevant for the cluster id
     *     retrieval
     * @return Cluster id identifying the cluster to deploy jobs to or null
     */
    @Nullable
    ClusterID getClusterId(Configuration configuration);

    /**
     * Returns the {@link ClusterSpecification} specified by the configuration and the command line
     * options. This specification can be used to deploy a new Flink cluster.
     *
     * @param configuration containing the configuration options relevant for the {@link
     *     ClusterSpecification}
     * @return the corresponding {@link ClusterSpecification} for a new Flink cluster
     */
    ClusterSpecification getClusterSpecification(Configuration configuration);

    /**
     * Returns the option to be used when trying to execute an application in Application Mode using
     * this cluster client factory, or an {@link Optional#empty()} if the environment of this
     * cluster client factory does not support Application Mode.
     */
    default Optional<String> getApplicationTargetName() {
        return Optional.empty();
    }
}
```

AbstractContainerizedClusterClientFactory  ClusterClientFactory抽象实现类 getClusterSpecification取了几个重要参数

jobManagerMemoryMB JobManagerOptions.TOTAL_PROCESS_MEMORY

taskManagerMemoryMB TaskManagerOptions.TOTAL_PROCESS_MEMORY

slotsPerTaskManager TaskManagerOptions.NUM_TASK_SLOTS

```java
/**
 * An abstract {@link ClusterClientFactory} containing some common implementations for different
 * containerized deployment clusters.
 */
@Internal
public abstract class AbstractContainerizedClusterClientFactory<ClusterID>
        implements ClusterClientFactory<ClusterID> {

    @Override
    public ClusterSpecification getClusterSpecification(Configuration configuration) {
        checkNotNull(configuration);

        final int jobManagerMemoryMB =
                JobManagerProcessUtils.processSpecFromConfigWithNewOptionToInterpretLegacyHeap(
                                configuration, JobManagerOptions.TOTAL_PROCESS_MEMORY)
                        .getTotalProcessMemorySize()
                        .getMebiBytes();

        final int taskManagerMemoryMB =
                TaskExecutorProcessUtils.processSpecFromConfig(
                                TaskExecutorProcessUtils
                                        .getConfigurationMapLegacyTaskManagerHeapSizeToConfigOption(
                                                configuration,
                                                TaskManagerOptions.TOTAL_PROCESS_MEMORY))
                        .getTotalProcessMemorySize()
                        .getMebiBytes();

        int slotsPerTaskManager = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS);

        return new ClusterSpecification.ClusterSpecificationBuilder()
                .setMasterMemoryMB(jobManagerMemoryMB)
                .setTaskManagerMemoryMB(taskManagerMemoryMB)
                .setSlotsPerTaskManager(slotsPerTaskManager)
                .createClusterSpecification();
    }
```

StandaloneClientFactory.isCompatibleWith

```java
@Override
public boolean isCompatibleWith(Configuration configuration) {
    checkNotNull(configuration);
    return RemoteExecutor.NAME.equalsIgnoreCase(
            configuration.getString(DeploymentOptions.TARGET));
}
```

YarnClusterClientFactory.isCompatibleWith

```java
@Override
public boolean isCompatibleWith(Configuration configuration) {
    checkNotNull(configuration);
    final String deploymentTarget = configuration.getString(DeploymentOptions.TARGET);
    return YarnDeploymentTarget.isValidYarnTarget(deploymentTarget);
}
```

```java
public static boolean isValidYarnTarget(final String configValue) {
    return configValue != null
            && Arrays.stream(YarnDeploymentTarget.values())
                    .anyMatch(
                            yarnDeploymentTarget ->
                                    yarnDeploymentTarget.name.equalsIgnoreCase(configValue));
}
```

```java
/** A class containing all the supported deployment target names for Yarn. */
@Internal
public enum YarnDeploymentTarget {
    SESSION("yarn-session"),
    APPLICATION("yarn-application"),

    @Deprecated
    PER_JOB("yarn-per-job");
}
```

KubernetesClusterClientFactory.isCompatibleWith类似 KubernetesDeploymentTarget

```java
/** A class containing all the supported deployment target names for Kubernetes. */
@Internal
public enum KubernetesDeploymentTarget {
    SESSION("kubernetes-session"),
    APPLICATION("kubernetes-application");
    }
```