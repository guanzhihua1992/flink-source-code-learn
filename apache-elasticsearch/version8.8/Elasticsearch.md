集群启动类 启动起始点 

```java
/**
 * Main entry point for starting elasticsearch.
 */
public static void main(final String[] args) {

    Bootstrap bootstrap = initPhase1();
    assert bootstrap != null;

    try {
        initPhase2(bootstrap);
        initPhase3(bootstrap);
    } catch (NodeValidationException e) {
        bootstrap.exitWithNodeValidationException(e);
    } catch (Throwable t) {
        bootstrap.exitWithUnknownException(t);
    }
}
```

分了三步走initPhase1();   initPhase2(bootstrap);  initPhase3(bootstrap);

```java
/**
 * First phase of process initialization.
 *
 * <p> Phase 1 consists of some static initialization, reading args from the CLI process, and
 * finally initializing logging. As little as possible should be done in this phase because
 * initializing logging is the last step.
 */
private static Bootstrap initPhase1() {
    final PrintStream out = getStdout();
    final PrintStream err = getStderr();
    final ServerArgs args;
    try {
        initSecurityProperties();

        /*
         * We want the JVM to think there is a security manager installed so that if internal policy decisions that would be based on
         * the presence of a security manager or lack thereof act as if there is a security manager present (e.g., DNS cache policy).
         * This forces such policies to take effect immediately.
         */
        org.elasticsearch.bootstrap.Security.setSecurityManager(new SecurityManager() {
            @Override
            public void checkPermission(Permission perm) {
                // grant all permissions so that we can later set the security manager to the one that we want
            }
        });
        LogConfigurator.registerErrorListener();

        BootstrapInfo.init();

        // note that reading server args does *not* close System.in, as it will be read from later for shutdown notification
        var in = new InputStreamStreamInput(System.in);
        args = new ServerArgs(in);

        // mostly just paths are used in phase 1, so secure settings are not needed
        Environment nodeEnv = new Environment(args.nodeSettings(), args.configDir());

        BootstrapInfo.setConsole(ConsoleLoader.loadConsole(nodeEnv));

        // DO NOT MOVE THIS
        // Logging must remain the last step of phase 1. Anything init steps needing logging should be in phase 2.
        LogConfigurator.setNodeName(Node.NODE_NAME_SETTING.get(args.nodeSettings()));
        LogConfigurator.configure(nodeEnv, args.quiet() == false);
    } catch (Throwable t) {
        // any exception this early needs to be fully printed and fail startup
        t.printStackTrace(err);
        err.flush();
        Bootstrap.exit(1); // mimic JDK exit code on exception
        return null; // unreachable, to satisfy compiler
    }

    return new Bootstrap(out, err, args);
}
```