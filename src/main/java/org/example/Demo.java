package org.example;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .databaseList("mydb") // set captured database
                .tableList("mydb.products", "mydb.orders") // set captured table
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .serverTimeZone("UTC")
                .build();

        //TODO 3,读取数据
        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource
                , WatermarkStrategy.noWatermarks(),
                "Mysql"
        );


        //TODO 4,打印
        mysqlDS.print(">>>>>>>>>");

        //TODO 5,启动
        env.execute();
    }
}