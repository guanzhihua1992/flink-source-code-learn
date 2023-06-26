Task中执行具体算子的计算逻辑是通过 AbstractInvokable Class触发的，AbstractInvokable是所有Task的 入口类，完成所有类型Task的加载与执行操作。

AbstractInvokable根据计算类型不同分为支持流 计算的StreamTask和支持批计算的BatchTask，而StreamTask中又含有 SourceStreamTask、OneInputStreamTask以及TwoInputStreamTask等 具体的StreamTask类型。在AbstractInvokable抽象类中主要通过 invoke()抽象方法触发Operator计算流程，调用cancel()方法停止 Task。对于AbstractInvokable.invoke()抽象方法则必须由子类实 现，cancel()方法则是选择性实现，在默认情况下不做任何操作。

可以说AbstractInvokable是所有StreamTask或BatchTask的入口 类，通过在TaskExecutor创建的Task线程中调用invoke()方法触发 StreamTask或BatchTask的运行。

在Task.loadAndInstantiateInvokable() 方法中定义了AbstractInvokable抽象类的加载过程， AbstractInvokable主要通过反射将指定className的 AbstractInvokable实现类加载到ClassLoader中

```java
invokable =
        loadAndInstantiateInvokable(
                userCodeClassLoader.asClassLoader(), nameOfInvokableClass, env);
```

```java
private static TaskInvokable loadAndInstantiateInvokable(
        ClassLoader classLoader, String className, Environment environment) throws Throwable {
// 通过反射方式创建invokableClass
    final Class<? extends TaskInvokable> invokableClass;
    try {
        invokableClass =
                Class.forName(className, true, classLoader).asSubclass(TaskInvokable.class);
    } catch (Throwable t) {
        throw new Exception("Could not load the task's invokable class.", t);
    }

    Constructor<? extends TaskInvokable> statelessCtor;

    try {// 创建invokableClass构造器
        statelessCtor = invokableClass.getConstructor(Environment.class);
    } catch (NoSuchMethodException ee) {
        throw new FlinkException("Task misses proper constructor", ee);
    }

    // instantiate the class
    try {// 通过构造器实例化invokableClass对象并返回
        //noinspection ConstantConditions  --> cannot happen
        return statelessCtor.newInstance(environment);
    } catch (InvocationTargetException e) {
        // directly forward exceptions from the eager initialization
        throw e.getTargetException();
    } catch (Exception e) {
        throw new FlinkException("Could not instantiate the task's invokable class.", e);
    }
}
```