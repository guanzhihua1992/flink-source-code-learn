集群公用的AkkaRpcService相当于Akka系统中的ActorSystem.管理集群中Actor.

创建过程 

1.AkkaRpcServiceUtils.createRemoteRpcService()

```java
// ------------------------------------------------------------------------
//  RPC instantiation
// ------------------------------------------------------------------------

static AkkaRpcService createRemoteRpcService(
        Configuration configuration,
        @Nullable String externalAddress,
        String externalPortRange,
        @Nullable String bindAddress,
        @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<Integer> bindPort)
        throws Exception {
    final AkkaRpcServiceBuilder akkaRpcServiceBuilder =
            AkkaRpcServiceUtils.remoteServiceBuilder(
                    configuration, externalAddress, externalPortRange);

    if (bindAddress != null) {
        akkaRpcServiceBuilder.withBindAddress(bindAddress);
    }

    bindPort.ifPresent(akkaRpcServiceBuilder::withBindPort);

    return akkaRpcServiceBuilder.createAndStart();
}
```

2.akkaRpcServiceBuilder.createAndStart()

```java
public AkkaRpcService createAndStart(
            TriFunction<ActorSystem, AkkaRpcServiceConfiguration, ClassLoader, AkkaRpcService>
                    constructor)
            throws Exception {
        if (actorSystemExecutorConfiguration == null) {
            actorSystemExecutorConfiguration =
                    AkkaUtils.getForkJoinExecutorConfig(
                            AkkaBootstrapTools.getForkJoinExecutorConfiguration(configuration));
        }

        final ActorSystem actorSystem;

        // akka internally caches the context class loader
        // make sure it uses the plugin class loader
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(getClass().getClassLoader())) {
            if (externalAddress == null) {
                // create local actor system
                actorSystem =
                        AkkaBootstrapTools.startLocalActorSystem(
                                configuration,
                                actorSystemName,
                                logger,
                                actorSystemExecutorConfiguration,
                                customConfig);
            } else {
                // create remote actor system
                actorSystem =
                        AkkaBootstrapTools.startRemoteActorSystem(
                                configuration,
                                actorSystemName,
                                externalAddress,
                                externalPortRange,
                                bindAddress,
                                Optional.ofNullable(bindPort),
                                logger,
                                actorSystemExecutorConfiguration,
                                customConfig);
            }
        }

        return constructor.apply(
                actorSystem,
                AkkaRpcServiceConfiguration.fromConfiguration(configuration),
                RpcService.class.getClassLoader());
    }
}
```

3.AkkaBootstrapTools.startLocalActorSystem() AkkaBootstrapTools.startRemoteActorSystem() 都是调用AkkaBootstrapTools.startActorSystem() 使用AkkaUtils.createActorSystem() 得到 ActorSystem.最后得到的是一个持有actorSystem的 AkkaRpcService::new 实例

```java
/**
 * Starts an Actor System with given Akka config.
 *
 * @param akkaConfig Config of the started ActorSystem.
 * @param actorSystemName Name of the started ActorSystem.
 * @param logger The logger to output log information.
 * @return The ActorSystem which has been started.
 */
private static ActorSystem startActorSystem(
        Config akkaConfig, String actorSystemName, Logger logger) {
    logger.debug("Using akka configuration\n {}", akkaConfig);
    ActorSystem actorSystem = AkkaUtils.createActorSystem(actorSystemName, akkaConfig);

    logger.info("Actor system started at {}", AkkaUtils.getAddress(actorSystem));
    return actorSystem;
}
```

ActorSystem 创建 AkkaUtils.createActorSystem()

```java
public static ActorSystem createActorSystem(String actorSystemName, Config akkaConfig) {
    // Initialize slf4j as logger of Akka's Netty instead of java.util.logging (FLINK-1650)
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    return RobustActorSystem.create(actorSystemName, akkaConfig);
}
```

RobustActorSystem.create()

```java
private static RobustActorSystem create(
        String name,
        ActorSystemSetup setup,
        Option<Thread.UncaughtExceptionHandler> uncaughtExceptionHandler) {
    final Optional<BootstrapSetup> bootstrapSettings = setup.get(BootstrapSetup.class);
    final ClassLoader classLoader = RobustActorSystem.class.getClassLoader();
    final Config appConfig =
            bootstrapSettings
                    .map(BootstrapSetup::config)
                    .flatMap(RobustActorSystem::toJavaOptional)
                    .orElseGet(() -> ConfigFactory.load(classLoader));
    final Option<ExecutionContext> defaultEC =
            toScalaOption(
                    bootstrapSettings
                            .map(BootstrapSetup::defaultExecutionContext)
                            .flatMap(RobustActorSystem::toJavaOptional));

    final RobustActorSystem robustActorSystem =
            new RobustActorSystem(name, appConfig, classLoader, defaultEC, setup) {
                @Override
                public Thread.UncaughtExceptionHandler uncaughtExceptionHandler() {
                    return uncaughtExceptionHandler.getOrElse(super::uncaughtExceptionHandler);
                }
            };
    robustActorSystem.start();
    return robustActorSystem;
}
```

robustActorSystem.start(); 

初始化过程中 startDeadLettersActor startSupervisorActor 分别启动了两个 ActorRef

```java
return constructor.apply(
        actorSystem,
        AkkaRpcServiceConfiguration.fromConfiguration(configuration),
        RpcService.class.getClassLoader());
```

```java
AkkaRpcService(
        final ActorSystem actorSystem,
        final AkkaRpcServiceConfiguration configuration,
        final ClassLoader flinkClassLoader) {
    this.actorSystem = checkNotNull(actorSystem, "actor system");
    this.configuration = checkNotNull(configuration, "akka rpc service configuration");
    this.flinkClassLoader = checkNotNull(flinkClassLoader, "flinkClassLoader");

    Address actorSystemAddress = AkkaUtils.getAddress(actorSystem);

    if (actorSystemAddress.host().isDefined()) {
        address = actorSystemAddress.host().get();
    } else {
        address = "";
    }

    if (actorSystemAddress.port().isDefined()) {
        port = (Integer) actorSystemAddress.port().get();
    } else {
        port = -1;
    }

    captureAskCallstacks = configuration.captureAskCallStack();

    // Akka always sets the threads context class loader to the class loader with which it was
    // loaded (i.e., the plugin class loader)
    // we must ensure that the context class loader is set to the Flink class loader when we
    // call into Flink
    // otherwise we could leak the plugin class loader or poison the context class loader of
    // external threads (because they inherit the current threads context class loader)
    internalScheduledExecutor =
            new ActorSystemScheduledExecutorAdapter(actorSystem, flinkClassLoader);

    terminationFuture = new CompletableFuture<>();

    stopped = false;

    supervisor = startSupervisorActor();
    startDeadLettersActor();
}
```

```java
private void startDeadLettersActor() {
    final ActorRef deadLettersActor =
            actorSystem.actorOf(DeadLettersActor.getProps(), "deadLettersActor");
    actorSystem.eventStream().subscribe(deadLettersActor, DeadLetter.class);
}
```

```java
private Supervisor startSupervisorActor() {
    final ExecutorService terminationFutureExecutor =
            Executors.newSingleThreadExecutor(
                    new ExecutorThreadFactory(
                            "AkkaRpcService-Supervisor-Termination-Future-Executor"));
    final ActorRef actorRef =
            SupervisorActor.startSupervisorActor(
                    actorSystem,
                    withContextClassLoader(terminationFutureExecutor, flinkClassLoader));

    return Supervisor.create(actorRef, terminationFutureExecutor);
}
```

启动集群中 RpcEndpoint 的 RpcServer 实例 RpcEndpoint构造函数

```java
protected RpcEndpoint(final RpcService rpcService, final String endpointId) {
    this.rpcService = checkNotNull(rpcService, "rpcService");
    this.endpointId = checkNotNull(endpointId, "endpointId");

    this.rpcServer = rpcService.startServer(this);
    this.resourceRegistry = new CloseableRegistry();

    this.mainThreadExecutor =
            new MainThreadExecutor(rpcServer, this::validateRunsInMainThread, endpointId);
    registerResource(this.mainThreadExecutor);
}
```

rpcService.startServer(this);

```java
@Override
public <C extends RpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint) {
    checkNotNull(rpcEndpoint, "rpc endpoint");

    final SupervisorActor.ActorRegistration actorRegistration =
            registerAkkaRpcActor(rpcEndpoint);
    final ActorRef actorRef = actorRegistration.getActorRef();
    final CompletableFuture<Void> actorTerminationFuture =
            actorRegistration.getTerminationFuture();

    LOG.info(
            "Starting RPC endpoint for {} at {} .",
            rpcEndpoint.getClass().getName(),
            actorRef.path());

    final String akkaAddress = AkkaUtils.getAkkaURL(actorSystem, actorRef);
    final String hostname;
    Option<String> host = actorRef.path().address().host();
    if (host.isEmpty()) {
        hostname = "localhost";
    } else {
        hostname = host.get();
    }

    Set<Class<?>> implementedRpcGateways =
            new HashSet<>(RpcUtils.extractImplementedRpcGateways(rpcEndpoint.getClass()));

    implementedRpcGateways.add(RpcServer.class);
    implementedRpcGateways.add(AkkaBasedEndpoint.class);

    final InvocationHandler akkaInvocationHandler;

    if (rpcEndpoint instanceof FencedRpcEndpoint) {
        // a FencedRpcEndpoint needs a FencedAkkaInvocationHandler
        akkaInvocationHandler =
                new FencedAkkaInvocationHandler<>(
                        akkaAddress,
                        hostname,
                        actorRef,
                        configuration.getTimeout(),
                        configuration.getMaximumFramesize(),
                        configuration.isForceRpcInvocationSerialization(),
                        actorTerminationFuture,
                        ((FencedRpcEndpoint<?>) rpcEndpoint)::getFencingToken,
                        captureAskCallstacks,
                        flinkClassLoader);
    } else {
        akkaInvocationHandler =
                new AkkaInvocationHandler(
                        akkaAddress,
                        hostname,
                        actorRef,
                        configuration.getTimeout(),
                        configuration.getMaximumFramesize(),
                        configuration.isForceRpcInvocationSerialization(),
                        actorTerminationFuture,
                        captureAskCallstacks,
                        flinkClassLoader);
    }

    // Rather than using the System ClassLoader directly, we derive the ClassLoader
    // from this class . That works better in cases where Flink runs embedded and all Flink
    // code is loaded dynamically (for example from an OSGI bundle) through a custom ClassLoader
    ClassLoader classLoader = getClass().getClassLoader();

    @SuppressWarnings("unchecked")
    RpcServer server =
            (RpcServer)
                    Proxy.newProxyInstance(
                            classLoader,
                            implementedRpcGateways.toArray(
                                    new Class<?>[implementedRpcGateways.size()]),
                            akkaInvocationHandler);

    return server;
}
```

先注册 registerAkkaRpcActor(rpcEndpoint) 然后是按条件生成akkaInvocationHandler 最后Proxy.newProxyInstance() 反射创建代理类  RpcServer 实例. 

```java
private <C extends RpcEndpoint & RpcGateway>
        SupervisorActor.ActorRegistration registerAkkaRpcActor(C rpcEndpoint) {
    final Class<? extends AbstractActor> akkaRpcActorType;

    if (rpcEndpoint instanceof FencedRpcEndpoint) {
        akkaRpcActorType = FencedAkkaRpcActor.class;
    } else {
        akkaRpcActorType = AkkaRpcActor.class;
    }

    synchronized (lock) {
        checkState(!stopped, "RpcService is stopped");

        final SupervisorActor.StartAkkaRpcActorResponse startAkkaRpcActorResponse =
                SupervisorActor.startAkkaRpcActor(
                        supervisor.getActor(),
                        actorTerminationFuture ->
                                Props.create(
                                        akkaRpcActorType,
                                        rpcEndpoint,
                                        actorTerminationFuture,
                                        getVersion(),
                                        configuration.getMaximumFramesize(),
                                        configuration.isForceRpcInvocationSerialization(),
                                        flinkClassLoader),
                        rpcEndpoint.getEndpointId());

        final SupervisorActor.ActorRegistration actorRegistration =
                startAkkaRpcActorResponse.orElseThrow(
                        cause ->
                                new AkkaRpcRuntimeException(
                                        String.format(
                                                "Could not create the %s for %s.",
                                                AkkaRpcActor.class.getSimpleName(),
                                                rpcEndpoint.getEndpointId()),
                                        cause));

        actors.put(actorRegistration.getActorRef(), rpcEndpoint);

        return actorRegistration;
    }
}
```

判断类型,锁代码块内完成启动信息通信,并将其引用放入actors

```java
public static StartAkkaRpcActorResponse startAkkaRpcActor(
        ActorRef supervisor, StartAkkaRpcActor.PropsFactory propsFactory, String endpointId) {
    return Patterns.ask(
                    supervisor,
                    createStartAkkaRpcActorMessage(propsFactory, endpointId),
                    RpcUtils.INF_DURATION)
            .toCompletableFuture()
            .thenApply(SupervisorActor.StartAkkaRpcActorResponse.class::cast)
            .join();
}

public static StartAkkaRpcActor createStartAkkaRpcActorMessage(
            StartAkkaRpcActor.PropsFactory propsFactory, String endpointId) {
        return StartAkkaRpcActor.create(propsFactory, endpointId);
    }
```

具体通信 AkkaInvocationHandler.invoke()

```java
@Override
public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    Class<?> declaringClass = method.getDeclaringClass();

    Object result;

    if (declaringClass.equals(AkkaBasedEndpoint.class)
            || declaringClass.equals(Object.class)
            || declaringClass.equals(RpcGateway.class)
            || declaringClass.equals(StartStoppable.class)
            || declaringClass.equals(MainThreadExecutable.class)
            || declaringClass.equals(RpcServer.class)) {
        result = method.invoke(this, args);
    } else if (declaringClass.equals(FencedRpcGateway.class)) {
        throw new UnsupportedOperationException(
                "AkkaInvocationHandler does not support the call FencedRpcGateway#"
                        + method.getName()
                        + ". This indicates that you retrieved a FencedRpcGateway without specifying a "
                        + "fencing token. Please use RpcService#connect(RpcService, F, Time) with F being the fencing token to "
                        + "retrieve a properly FencedRpcGateway.");
    } else {
        result = invokeRpc(method, args);
    }

    return result;
}

private Object invokeRpc(Method method, Object[] args) throws Exception {
        String methodName = method.getName();
        Class<?>[] parameterTypes = method.getParameterTypes();
        final boolean isLocalRpcInvocation = method.getAnnotation(Local.class) != null;
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        Duration futureTimeout =
                RpcGatewayUtils.extractRpcTimeout(parameterAnnotations, args, timeout);

        final RpcInvocation rpcInvocation =
                createRpcInvocationMessage(
                        method.getDeclaringClass().getSimpleName(),
                        methodName,
                        isLocalRpcInvocation,
                        parameterTypes,
                        args);

        Class<?> returnType = method.getReturnType();

        final Object result;

        if (Objects.equals(returnType, Void.TYPE)) {
            tell(rpcInvocation);

            result = null;
        } else {
            // Capture the call stack. It is significantly faster to do that via an exception than
            // via Thread.getStackTrace(), because exceptions lazily initialize the stack trace,
            // initially only
            // capture a lightweight native pointer, and convert that into the stack trace lazily
            // when needed.
            final Throwable callStackCapture = captureAskCallStack ? new Throwable() : null;

            // execute an asynchronous call
            final CompletableFuture<?> resultFuture =
                    ask(rpcInvocation, futureTimeout)
                            .thenApply(
                                    resultValue ->
                                            deserializeValueIfNeeded(
                                                    resultValue, method, flinkClassLoader));

            final CompletableFuture<Object> completableFuture = new CompletableFuture<>();
            resultFuture.whenComplete(
                    (resultValue, failure) -> {
                        if (failure != null) {
                            completableFuture.completeExceptionally(
                                    resolveTimeoutException(
                                            ExceptionUtils.stripCompletionException(failure),
                                            callStackCapture,
                                            address,
                                            rpcInvocation));
                        } else {
                            completableFuture.complete(resultValue);
                        }
                    });

            if (Objects.equals(returnType, CompletableFuture.class)) {
                result = completableFuture;
            } else {
                try {
                    result = completableFuture.get(futureTimeout.toMillis(), TimeUnit.MILLISECONDS);
                } catch (ExecutionException ee) {
                    throw new RpcException(
                            "Failure while obtaining synchronous RPC result.",
                            ExceptionUtils.stripExecutionException(ee));
                }
            }
        }

        return result;
    }
```

AkkaInvocationHandler.tell()

```
protected void tell(Object message) {
    rpcEndpoint.tell(message, ActorRef.noSender());
}
```