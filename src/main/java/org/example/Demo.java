package org.example;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class Demo {
    public static void main(String[] args) throws Exception {
        //TODO 1,获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);


        //开启CK
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);


        env.setStateBackend(new HashMapStateBackend());


        //TODO 2,使用FlinkCDC构建Source
        MySqlSource<SourceRow> mySqlSource = MySqlSource.<SourceRow>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .databaseList("mydb") // set captured database
                .tableList("mydb.orders") // set captured table
                .username("root")
                .password("root")
                .deserializer(new SourceRowDeserializer()) // converts SourceRecord to JSON String
                .serverTimeZone("UTC")
                .build();

        DataStreamSource<SourceRow> orderSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql");

        SingleOutputStreamOperator<SourceRow> orderAll = AsyncDataStream.unorderedWait(
                orderSource, new ProductDimAsyncFunction("mydb.products"), 60, TimeUnit.SECONDS);



        var sourceRows = orderAll
                .setParallelism(1)
                .keyBy(SourceRow::getTable);

        sourceRows.print();
        sourceRows.map(getRowMapFunction())
                .print();
//                .addSink(new TargetRowSinkFunction());

        env.execute("BinlogJob");
    }

    private static MapFunction<SourceRow, TargetRow> getRowMapFunction() {
        return row -> {
            var primaryKeys = new ArrayList<String>();
            primaryKeys.add("id");

            return new TargetRow(
                    "es_orders",
                    primaryKeys,
                    row.getColumns(),
                    row.getRowKind());
        };
    }

    private static String getProductTableDDL() {
        return "CREATE TABLE products (\n" +
                "    id INT,\n" +
                "    name STRING,\n" +
                "    description STRING,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                "  ) WITH (\n" +
                "   'server-time-zone'='UTC',\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://127.0.0.1:3306/mydb',\n" +
                "    'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'root',\n" +
                "    'table-name' = 'mydb.products'\n" +
                "  );";
    }
}