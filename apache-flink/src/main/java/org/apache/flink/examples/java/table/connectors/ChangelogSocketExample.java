package org.apache.flink.examples.java.table.connectors;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public final class ChangelogSocketExample {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String hostname = params.get("hostname", "localhost");
        final String port = params.get("port", "9999");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // source only supports parallelism of 1

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // register a table in the catalog
        tEnv.executeSql(
                "CREATE TABLE UserScores (name STRING, score INT)\n"
                        + "WITH (\n"
                        + "  'connector' = 'socket',\n"
                        + "  'hostname' = '"
                        + hostname
                        + "',\n"
                        + "  'port' = '"
                        + port
                        + "',\n"
                        + "  'byte-delimiter' = '10',\n"
                        + "  'format' = 'changelog-csv',\n"
                        + "  'changelog-csv.column-delimiter' = '|'\n"
                        + ")");

        // define a dynamic aggregating query
        final Table result = tEnv.sqlQuery("SELECT name, SUM(score) FROM UserScores GROUP BY name");

        // print the result to the console
        tEnv.toChangelogStream(result).print();

        env.execute();
    }
}
