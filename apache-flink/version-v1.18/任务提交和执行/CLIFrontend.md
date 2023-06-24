bin/flink run application.jar命令启动应用程序 时，会调用CLIFrontend中的main()方法执行CLIFrontend应用程序.

```java
/** Submits the job based on the arguments. */
public static void main(final String[] args) {
    int retCode = INITIAL_RET_CODE;
    try {
        retCode = mainInternal(args);
    } finally {
        System.exit(retCode);
    }
}
```

```java
@VisibleForTesting
static int mainInternal(final String[] args) {
    EnvironmentInformation.logEnvironmentInfo(LOG, "Command Line Client", args);

    // 1. find the configuration directory
    final String configurationDirectory = getConfigurationDirectoryFromEnv();

    // 2. load the global configuration 配置
    final Configuration configuration =
            GlobalConfiguration.loadConfiguration(configurationDirectory);

    // 3. load the custom command lines 命令行
    final List<CustomCommandLine> customCommandLines =
            loadCustomCommandLines(configuration, configurationDirectory);

    int retCode = INITIAL_RET_CODE;
    try {
        final CliFrontend cli = new CliFrontend(configuration, customCommandLines);
        CommandLine commandLine =
                cli.getCommandLine(
                        new Options(),
                        Arrays.copyOfRange(args, min(args.length, 1), args.length),
                        true);
        Configuration securityConfig = new Configuration(cli.configuration);
        DynamicPropertiesUtil.encodeDynamicProperties(commandLine, securityConfig);
        SecurityUtils.install(new SecurityConfiguration(securityConfig));
        retCode = SecurityUtils.getInstalledContext().runSecured(() -> cli.parseAndRun(args));
    } catch (Throwable t) {
        final Throwable strippedThrowable =
                ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
        LOG.error("Fatal error while running command line interface.", strippedThrowable);
        strippedThrowable.printStackTrace();
    }
    return retCode;
}
```

() -> cli.parseAndRun(args) callable 执行

```java
/**
 * Parses the command line arguments and starts the requested action.
 *
 * @param args command line arguments of the client.
 * @return The return code of the program
 */
public int parseAndRun(String[] args) {

    // check for action
    if (args.length < 1) {
        CliFrontendParser.printHelp(customCommandLines);
        System.out.println("Please specify an action.");
        return 1;
    }

    // get action
    String action = args[0];

    // remove action from parameters
    final String[] params = Arrays.copyOfRange(args, 1, args.length);

    try {
        // do action
        switch (action) {
            case ACTION_RUN:
                run(params);
                return 0;
            case ACTION_RUN_APPLICATION:
                runApplication(params);
                return 0;
            case ACTION_LIST:
                list(params);
                return 0;
            case ACTION_INFO:
                info(params);
                return 0;
            case ACTION_CANCEL:
                cancel(params);
                return 0;
            case ACTION_STOP:
                stop(params);
                return 0;
            case ACTION_SAVEPOINT:
                savepoint(params);
                return 0;
            case "-h":
            case "--help":
                CliFrontendParser.printHelp(customCommandLines);
                return 0;
            case "-v":
            case "--version":
                String version = EnvironmentInformation.getVersion();
                String commitID = EnvironmentInformation.getRevisionInformation().commitId;
                System.out.print("Version: " + version);
                System.out.println(
                        commitID.equals(EnvironmentInformation.UNKNOWN)
                                ? ""
                                : ", Commit ID: " + commitID);
                return 0;
            default:
                System.out.printf("\"%s\" is not a valid action.\n", action);
                System.out.println();
                System.out.println(
                        "Valid actions are \"run\", \"run-application\", \"list\", \"info\", \"savepoint\", \"stop\", or \"cancel\".");
                System.out.println();
                System.out.println(
                        "Specify the version option (-v or --version) to print Flink version.");
                System.out.println();
                System.out.println(
                        "Specify the help option (-h or --help) to get help on the command.");
                return 1;
        }
    } catch (CliArgsException ce) {
        return handleArgException(ce);
    } catch (ProgramParametrizationException ppe) {
        return handleParametrizationException(ppe);
    } catch (ProgramMissingJobException pmje) {
        return handleMissingJobException();
    } catch (Exception e) {
        return handleError(e);
    }
}
```

run(params);

```java
/**
 * Executions the run action.
 *
 * @param args Command line arguments for the run action.
 */
protected void run(String[] args) throws Exception {
    LOG.info("Running 'run' command.");
//命令行参数
    final Options commandOptions = CliFrontendParser.getRunCommandOptions();
    final CommandLine commandLine = getCommandLine(commandOptions, args, true);

    // evaluate help flag
    if (commandLine.hasOption(HELP_OPTION.getOpt())) {
        CliFrontendParser.printHelpForRun(customCommandLines);
        return;
    }
//校验
    final CustomCommandLine activeCommandLine =
            validateAndGetActiveCommandLine(checkNotNull(commandLine));
//命令行参数 转 ProgramOptions
    final ProgramOptions programOptions = ProgramOptions.create(commandLine);
//用户上传的 jar以及依赖
    final List<URL> jobJars = getJobJarAndDependencies(programOptions);
//
    final Configuration effectiveConfiguration =
            getEffectiveConfiguration(activeCommandLine, commandLine, programOptions, jobJars);

    LOG.debug("Effective executor configuration: {}", effectiveConfiguration);

    try (PackagedProgram program = getPackagedProgram(programOptions, effectiveConfiguration)) {
        executeProgram(effectiveConfiguration, program);
    }
}
```

getPackagedProgram

```java
private PackagedProgram getPackagedProgram(
        ProgramOptions programOptions, Configuration effectiveConfiguration)
        throws ProgramInvocationException, CliArgsException {
    PackagedProgram program;
    try {
        LOG.info("Building program from JAR file");
        program = buildProgram(programOptions, effectiveConfiguration);
    } catch (FileNotFoundException e) {
        throw new CliArgsException(
                "Could not build the program from JAR file: " + e.getMessage(), e);
    }
    return program;
}
```

executeProgram

```java
protected void executeProgram(final Configuration configuration, final PackagedProgram program)
        throws ProgramInvocationException {
    ClientUtils.executeProgram(
            new DefaultExecutorServiceLoader(), configuration, program, false, false);
}
```

ClientUtils.executeProgram()

```java
public static void executeProgram(
        PipelineExecutorServiceLoader executorServiceLoader,
        Configuration configuration,
        PackagedProgram program,
        boolean enforceSingleJobExecution,
        boolean suppressSysout)
        throws ProgramInvocationException {
    checkNotNull(executorServiceLoader);
    final ClassLoader userCodeClassLoader = program.getUserCodeClassLoader();
    final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    try {// 将当前线程的ContextClassLoader设定为userCodeClassLoader
        Thread.currentThread().setContextClassLoader(userCodeClassLoader);

        LOG.info(
                "Starting program (detached: {})",
                !configuration.getBoolean(DeploymentOptions.ATTACHED));

        ContextEnvironment.setAsContext(
                executorServiceLoader,
                configuration,
                userCodeClassLoader,
                enforceSingleJobExecution,
                suppressSysout);

        StreamContextEnvironment.setAsContext(
                executorServiceLoader,
                configuration,
                userCodeClassLoader,
                enforceSingleJobExecution,
                suppressSysout);

        try {
            program.invokeInteractiveModeForExecution();
        } finally {//资源释放
            ContextEnvironment.unsetAsContext();
            StreamContextEnvironment.unsetAsContext();
        }
    } finally {
        Thread.currentThread().setContextClassLoader(contextClassLoader);
    }
}
```

 PackagedProgram program.invokeInteractiveModeForExecution();

```java
public void invokeInteractiveModeForExecution() throws ProgramInvocationException {
    FlinkSecurityManager.monitorUserSystemExitForCurrentThread();//监控
    try {
        callMainMethod(mainClass, args);
    } finally {
        FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
    }
}
```

callMainMethod(mainClass, args);

```java
private static void callMainMethod(Class<?> entryClass, String[] args)
        throws ProgramInvocationException {
    Method mainMethod;
    if (!Modifier.isPublic(entryClass.getModifiers())) {
        throw new ProgramInvocationException(
                "The class " + entryClass.getName() + " must be public.");
    }

    try {
        mainMethod = entryClass.getMethod("main", String[].class);
    } catch (NoSuchMethodException e) {
        throw new ProgramInvocationException(
                "The class " + entryClass.getName() + " has no main(String[]) method.");
    } catch (Throwable t) {
        throw new ProgramInvocationException(
                "Could not look up the main(String[]) method from the class "
                        + entryClass.getName()
                        + ": "
                        + t.getMessage(),
                t);
    }

    if (!Modifier.isStatic(mainMethod.getModifiers())) {
        throw new ProgramInvocationException(
                "The class " + entryClass.getName() + " declares a non-static main method.");
    }
    if (!Modifier.isPublic(mainMethod.getModifiers())) {
        throw new ProgramInvocationException(
                "The class " + entryClass.getName() + " declares a non-public main method.");
    }

    try {
        mainMethod.invoke(null, (Object) args);
    } catch (IllegalArgumentException e) {
        throw new ProgramInvocationException(
                "Could not invoke the main method, arguments are not matching.", e);
    } catch (IllegalAccessException e) {
        throw new ProgramInvocationException(
                "Access to the main method was denied: " + e.getMessage(), e);
    } catch (InvocationTargetException e) {
        Throwable exceptionInMethod = e.getTargetException();
        if (exceptionInMethod instanceof Error) {
            throw (Error) exceptionInMethod;
        } else if (exceptionInMethod instanceof ProgramParametrizationException) {
            throw (ProgramParametrizationException) exceptionInMethod;
        } else if (exceptionInMethod instanceof ProgramInvocationException) {
            throw (ProgramInvocationException) exceptionInMethod;
        } else {
            throw new ProgramInvocationException(
                    "The main method caused an error: " + exceptionInMethod.getMessage(),
                    exceptionInMethod);
        }
    } catch (Throwable t) {
        throw new ProgramInvocationException(
                "An error occurred while invoking the program's main method: " + t.getMessage(),
                t);
    }
}
```

```java
mainMethod.invoke(null, (Object) args); 反射执行
```

回头看下 PackagedProgram mainClass, args 怎么来的 PackagedProgram program = getPackagedProgram(programOptions, effectiveConfiguration)

```java
private PackagedProgram getPackagedProgram(
        ProgramOptions programOptions, Configuration effectiveConfiguration)
        throws ProgramInvocationException, CliArgsException {
    PackagedProgram program;
    try {
        LOG.info("Building program from JAR file");
        program = buildProgram(programOptions, effectiveConfiguration);
    } catch (FileNotFoundException e) {
        throw new CliArgsException(
                "Could not build the program from JAR file: " + e.getMessage(), e);
    }
    return program;
}
```

program = buildProgram(programOptions, effectiveConfiguration);

```java
/**
 * Creates a Packaged program from the given command line options and the
 * effectiveConfiguration.
 *
 * @return A PackagedProgram (upon success)
 */
PackagedProgram buildProgram(final ProgramOptions runOptions, final Configuration configuration)
        throws FileNotFoundException, ProgramInvocationException, CliArgsException {
    runOptions.validate();

    String[] programArgs = runOptions.getProgramArgs();
    String jarFilePath = runOptions.getJarFilePath();
    List<URL> classpaths = runOptions.getClasspaths();

    // Get assembler class
    String entryPointClass = runOptions.getEntryPointClassName();
    File jarFile = jarFilePath != null ? getJarFile(jarFilePath) : null;

    return PackagedProgram.newBuilder()
            .setJarFile(jarFile)
            .setUserClassPaths(classpaths)
            .setEntryPointClassName(entryPointClass)
            .setConfiguration(configuration)
            .setSavepointRestoreSettings(runOptions.getSavepointRestoreSettings())
            .setArguments(programArgs)
            .build();
}
```

build

```
public PackagedProgram build() throws ProgramInvocationException {
    if (jarFile == null && entryPointClassName == null) {
        throw new IllegalArgumentException(
                "The jarFile and entryPointClassName can not be null at the same time.");
    }
    return new PackagedProgram(
            jarFile,
            userClassPaths,
            entryPointClassName,
            configuration,
            savepointRestoreSettings,
            args);
}
```



```java
/**
 * Creates an instance that wraps the plan defined in the jar file using the given arguments.
 * For generating the plan the class defined in the className parameter is used.
 *
 * @param jarFile The jar file which contains the plan.
 * @param classpaths Additional classpath URLs needed by the Program.
 * @param entryPointClassName Name of the class which generates the plan. Overrides the class
 *     defined in the jar file manifest.
 * @param configuration Flink configuration which affects the classloading policy of the Program
 *     execution.
 * @param args Optional. The arguments used to create the pact plan, depend on implementation of
 *     the pact plan. See getDescription().
 * @throws ProgramInvocationException This invocation is thrown if the Program can't be properly
 *     loaded. Causes may be a missing / wrong class or manifest files.
 */
private PackagedProgram(
        @Nullable File jarFile,
        List<URL> classpaths,
        @Nullable String entryPointClassName,
        Configuration configuration,
        SavepointRestoreSettings savepointRestoreSettings,
        String... args)
        throws ProgramInvocationException {
    this.classpaths = checkNotNull(classpaths);
    this.savepointSettings = checkNotNull(savepointRestoreSettings);
    this.args = checkNotNull(args);

    checkArgument(
            jarFile != null || entryPointClassName != null,
            "Either the jarFile or the entryPointClassName needs to be non-null.");

    // whether the job is a Python job.
    this.isPython = isPython(entryPointClassName);

    // load the jar file if exists
    this.jarFile = loadJarFile(jarFile);

    assert this.jarFile != null || entryPointClassName != null;

    // now that we have an entry point, we can extract the nested jar files (if any)
    this.extractedTempLibraries =
            this.jarFile == null
                    ? Collections.emptyList()
                    : extractContainedLibraries(this.jarFile);
    this.userCodeClassLoader =
            ClientUtils.buildUserCodeClassLoader(
                    getJobJarAndDependencies(),
                    classpaths,
                    getClass().getClassLoader(),
                    configuration);

    // load the entry point class main函数
    this.mainClass =
            loadMainClass(
                    // if no entryPointClassName name was given, we try and look one up through
                    // the manifest
                    entryPointClassName != null
                            ? entryPointClassName
                            : getEntryPointClassNameFromJar(this.jarFile),
                    userCodeClassLoader);

    if (!hasMainMethod(mainClass)) {
        throw new ProgramInvocationException(
                "The given program class does not have a main(String[]) method.");
    }
}
```