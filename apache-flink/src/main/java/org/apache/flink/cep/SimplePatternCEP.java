package org.apache.flink.cep;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimplePatternCEP {
    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        // set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<Event> input =
                env.fromElements(
                        new Event(1, "barfoo", 1.0),
                        new Event(2, "start", 2.0),
                        new Event(3, "foobar", 3.0),
                        new SubEvent(4, "foo", 4.0, 1.0),
                        new Event(5, "middle", 5.0),
                        new SubEvent(6, "middle", 6.0, 2.0),
                        new SubEvent(7, "bar", 3.0, 3.0),
                        new Event(42, "42", 42.0),
                        new Event(8, "end", 1.0));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("start");
                                    }
                                })
                        .followedByAny("middle")
                        .subtype(SubEvent.class)
                        .where(
                                new SimpleCondition<SubEvent>() {

                                    @Override
                                    public boolean filter(SubEvent value) throws Exception {
                                        return value.getName().equals("middle");
                                    }
                                })
                        .followedByAny("end")
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("end");
                                    }
                                });

        DataStream<String> result =
                CEP.pattern(input, pattern)
                        .inProcessingTime()
                        .flatSelect(
                                (p, o) -> {
                                    StringBuilder builder = new StringBuilder();

                                    builder.append(p.get("start").get(0).getId())
                                            .append(",")
                                            .append(p.get("middle").get(0).getId())
                                            .append(",")
                                            .append(p.get("end").get(0).getId());

                                    o.collect(builder.toString());
                                },
                                Types.STRING);

        // emit result
        if (params.has("output")) {
            result.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            result.print();
        }
        // execute program
        env.execute("Streaming SimplePatternCEP");
    }
}
