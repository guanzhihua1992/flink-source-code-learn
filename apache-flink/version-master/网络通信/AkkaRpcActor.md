消息接收

创建接收器 AkkaRpcActor.createReceive() 三类消息 RemoteHandshakeMessage 主要用于进行正式RPC通信之前的网络连接检测，保障RPC通信正常。 ControlMessages用于控制Akka系统，例如启动和停止Akka Actor等控 制消息.第三类集群运行时中RPC 组件通信使用的Message类型.

```java
@Override
public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(RemoteHandshakeMessage.class, this::handleHandshakeMessage)
            .match(ControlMessages.class, this::handleControlMessage)
            .matchAny(this::handleMessage)
            .build();
}
```

接收处理消息 handleMessage

```java
private void handleMessage(final Object message) {
    if (state.isRunning()) {
        mainThreadValidator.enterMainThread();

        try {
            handleRpcMessage(message);
        } finally {
            mainThreadValidator.exitMainThread();
        }
    } else {
        log.info(
                "The rpc endpoint {} has not been started yet. Discarding message {} until processing is started.",
                rpcEndpoint.getClass().getName(),
                message);

        sendErrorIfSender(
                new EndpointNotStartedException(
                        String.format(
                                "Discard message %s, because the rpc endpoint %s has not been started yet.",
                                message, rpcEndpoint.getAddress())));
    }
}
```

```java
protected void handleRpcMessage(Object message) {
    if (message instanceof RunAsync) {
        handleRunAsync((RunAsync) message);
    } else if (message instanceof CallAsync) {
        handleCallAsync((CallAsync) message);
    } else if (message instanceof RpcInvocation) {
        handleRpcInvocation((RpcInvocation) message);
    } else {
        log.warn(
                "Received message of unknown type {} with value {}. Dropping this message!",
                message.getClass().getName(),
                message);

        sendErrorIfSender(
                new AkkaUnknownMessageException(
                        "Received unknown message "
                                + message
                                + " of type "
                                + message.getClass().getSimpleName()
                                + '.'));
    }
}
```

```java
private void handleRpcInvocation(RpcInvocation rpcInvocation) {
    Method rpcMethod = null;

    try {
        String methodName = rpcInvocation.getMethodName();
        Class<?>[] parameterTypes = rpcInvocation.getParameterTypes();

        rpcMethod = lookupRpcMethod(methodName, parameterTypes);
    } catch (final NoSuchMethodException e) {
        log.error("Could not find rpc method for rpc invocation.", e);

        RpcConnectionException rpcException =
                new RpcConnectionException("Could not find rpc method for rpc invocation.", e);
        getSender().tell(new Status.Failure(rpcException), getSelf());
    }

    if (rpcMethod != null) {
        try {
            // this supports declaration of anonymous classes
            rpcMethod.setAccessible(true);

            final Method capturedRpcMethod = rpcMethod;
            if (rpcMethod.getReturnType().equals(Void.TYPE)) {
                // No return value to send back
                runWithContextClassLoader(
                        () -> capturedRpcMethod.invoke(rpcEndpoint, rpcInvocation.getArgs()),
                        flinkClassLoader);
            } else {
                final Object result;
                try {
                    result =
                            runWithContextClassLoader(
                                    () ->
                                            capturedRpcMethod.invoke(
                                                    rpcEndpoint, rpcInvocation.getArgs()),
                                    flinkClassLoader);
                } catch (InvocationTargetException e) {
                    log.debug(
                            "Reporting back error thrown in remote procedure {}", rpcMethod, e);

                    // tell the sender about the failure
                    getSender().tell(new Status.Failure(e.getTargetException()), getSelf());
                    return;
                }

                final String methodName = rpcMethod.getName();
                final boolean isLocalRpcInvocation =
                        rpcMethod.getAnnotation(Local.class) != null;

                if (result instanceof CompletableFuture) {
                    final CompletableFuture<?> responseFuture = (CompletableFuture<?>) result;
                    sendAsyncResponse(responseFuture, methodName, isLocalRpcInvocation);
                } else {
                    sendSyncResponse(result, methodName, isLocalRpcInvocation);
                }
            }
        } catch (Throwable e) {
            log.error("Error while executing remote procedure call {}.", rpcMethod, e);
            // tell the sender about the failure
            getSender().tell(new Status.Failure(e), getSelf());
        }
    }
}
```