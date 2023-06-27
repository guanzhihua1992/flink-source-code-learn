StreamTask中对输入数据和输出数据集的处理是通过 StreamInputProcessor完成的，StreamInputProcessor根据 StreamTask种类的不同，也分为StreamOneInputProcessor和 StreamTwoInputProcessor两种。

StreamInputProcessor实际上包含 StreamTaskInput和DataOutput两个组成部分，其中StreamTaskInput 的实现主要有StreamTaskSourceInput和StreamTaskNetworkInput两种 类型，StreamTaskSourceInput对应外部DataSource数据源的 StreamTaskInput，StreamTaskNetworkInput是算子之间网络传递对应 的StreamTaskInput。

从StreamOneInputProcessor.processInput()开始

```java
@Override
public DataInputStatus processInput() throws Exception {
    DataInputStatus status = input.emitNext(output);

    if (status == DataInputStatus.END_OF_DATA) {
        endOfInputAware.endInput(input.getInputIndex() + 1);
        output = new FinishedDataOutput<>();
    } else if (status == DataInputStatus.END_OF_RECOVERY) {
        if (input instanceof RecoverableStreamTaskInput) {
            input = ((RecoverableStreamTaskInput<IN>) input).finishRecovery();
        }
        return DataInputStatus.MORE_AVAILABLE;
    }

    return status;
}
```

DataInputStatus status = input.emitNext(output);

```java
@Override
public DataInputStatus emitNext(DataOutput<T> output) throws Exception {

    while (true) {
        // get the stream element from the deserializer
      //解析出来的StreamRecord数据会存放在 currentRecorddeserializer实例中，通过判断 currentRecorddeserializer是否为空，从 currentRecorddeserializer获取DeserializationResult。
        if (currentRecordDeserializer != null) {
            RecordDeserializer.DeserializationResult result;
            try {//序列化器获取stream element
                result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
            } catch (IOException e) {
                throw new IOException(
                        String.format("Can't get next record for channel %s", lastChannel), e);
            }
          // 如果Buffer已经被消费，则对Buffer数据占用的内存空间进行回收
            if (result.isBufferConsumed()) {
                currentRecordDeserializer = null;
            }
//如果获取结果是完整的Record记录，则调用processElement() 方法对数据元素进行处理。
            if (result.isFullRecord()) {
                processElement(deserializationDelegate.getInstance(), output);
                if (canEmitBatchOfRecords.check()) {
                    continue;
                }//处理完毕后退出循环并返回MORE_AVAILABLE状态，继续等待新 的数据接入。
                return DataInputStatus.MORE_AVAILABLE;
            }
        }
//调用checkpointedInputGate.pollNext()方法获取数据，这里可以理解为数据输入的门户。从 网络接入的数据是通过InputGate的InputChannel接入的。
      //从checkpointedInputGate中拉取数据
        Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();
        if (bufferOrEvent.isPresent()) {
            // return to the mailbox after receiving a checkpoint barrier to avoid processing of
            // data after the barrier before checkpoint is performed for unaligned checkpoint
            // mode
          //从InputGate中接入的数据格式为Optional bufferOrEvent 换句话讲，网络传输的数据既含有Buffer类型数据也含有事件数据，
            if (bufferOrEvent.get().isBuffer()) {
                processBuffer(bufferOrEvent.get());//处理Buffer类型数据
            } else {
                DataInputStatus status = processEvent(bufferOrEvent.get());//处理事件数据
                if (status == DataInputStatus.MORE_AVAILABLE && canEmitBatchOfRecords.check()) {
                    continue;
                }
                return status;
            }
        } else {
          // 如果checkpointedInputGate中已经没有数据，则返回END_OF_INPUT结束计算 否则返回NOTHING_AVAILABLE
            if (checkpointedInputGate.isFinished()) {
                checkState(
                        checkpointedInputGate.getAvailableFuture().isDone(),
                        "Finished BarrierHandler should be available");
                return DataInputStatus.END_OF_INPUT;
            }
            return DataInputStatus.NOTHING_AVAILABLE;
        }
    }
}
```

processElement(deserializationDelegate.getInstance(), output);StreamElement主要有两种类型:Event和 StreamRecord，其中Event包括Watermark、StreamStatus、 LatencyMarker等实现，业务数据元素则主要是StreamRecord类型。在 StreamTaskNetworkInput中会根据具体的StreamElement类型选择不同 的方法进行后续的处理。

```java
private void processElement(StreamElement recordOrMark, DataOutput<T> output) throws Exception {
    if (recordOrMark.isRecord()) {
      // 处理StreamRecord类型数据
        output.emitRecord(recordOrMark.asRecord());
    } else if (recordOrMark.isWatermark()) {
      // 处理Watermark类型数据
        statusWatermarkValve.inputWatermark(
                recordOrMark.asWatermark(), flattenedChannelIndices.get(lastChannel), output);
    } else if (recordOrMark.isLatencyMarker()) {
      // 处理LatencyMarker类型数据
        output.emitLatencyMarker(recordOrMark.asLatencyMarker());
    } else if (recordOrMark.isWatermarkStatus()) {
      // 处理StreamStatus类型数据
        statusWatermarkValve.inputWatermarkStatus(
                recordOrMark.asWatermarkStatus(),
                flattenedChannelIndices.get(lastChannel),
                output);
    } else {
        throw new UnsupportedOperationException("Unknown type of StreamElement");
    }
}
```

processBuffer(bufferOrEvent.get());

```java
protected void processBuffer(BufferOrEvent bufferOrEvent) throws IOException {
    lastChannel = bufferOrEvent.getChannelInfo();
    checkState(lastChannel != null);
    currentRecordDeserializer = getActiveSerializer(bufferOrEvent.getChannelInfo());
    checkState(
            currentRecordDeserializer != null,
            "currentRecordDeserializer has already been released");

    currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
}
```

DataInputStatus status = processEvent(bufferOrEvent.get());

```java
protected DataInputStatus processEvent(BufferOrEvent bufferOrEvent) {
    // Event received
    final AbstractEvent event = bufferOrEvent.getEvent();
    if (event.getClass() == EndOfData.class) {
        switch (checkpointedInputGate.hasReceivedEndOfData()) {
            case NOT_END_OF_DATA:
                // skip
                break;
            case DRAINED:
                return DataInputStatus.END_OF_DATA;
            case STOPPED:
                return DataInputStatus.STOPPED;
        }
    } else if (event.getClass() == EndOfPartitionEvent.class) {
        // release the record deserializer immediately,
        // which is very valuable in case of bounded stream
        releaseDeserializer(bufferOrEvent.getChannelInfo());
        if (checkpointedInputGate.isFinished()) {
            return DataInputStatus.END_OF_INPUT;
        }
    } else if (event.getClass() == EndOfChannelStateEvent.class) {
        if (checkpointedInputGate.allChannelsRecovered()) {
            return DataInputStatus.END_OF_RECOVERY;
        }
    }
    return DataInputStatus.MORE_AVAILABLE;
}
```

数据会在Task中 接入并处理，最终产生计算结果，通过Sink Operator发送到外部系 统。