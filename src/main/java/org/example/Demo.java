package org.example;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class Demo {
    public static void main(String[] args) throws Exception {
        //TODO 1,获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


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

        ElasticsearchSink<TargetRow> esSink = new Elasticsearch7SinkBuilder<TargetRow>()
                .setBulkFlushMaxActions(1) // Instructs the sink to emit after every element, otherwise they would be buffered
                .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
                .setEmitter(
                        (element, context, indexer) ->
                                indexer.add(createIndexRequest(element)))
                .build();


        var sourceRows = orderAll
                .setParallelism(1)
                .keyBy(SourceRow::getTable);

        sourceRows.print();
        sourceRows.map(getRowMapFunction())
                .sinkTo(esSink);

        env.execute("BinlogJob");
    }

    private static IndexRequest createIndexRequest(TargetRow element) {
        Map<String, Object> json = new HashMap<>(element.getColumns());
        return Requests.indexRequest()
                .index(element.getTable())
                .id(element.getPrimaryKeys().get(0))
                .source(json);
    }

    private static MapFunction<SourceRow, TargetRow> getRowMapFunction() {
        return row -> {
            var primaryKeys = new ArrayList<String>();
            primaryKeys.add(row.getColumns().get("order_id"));

            return new TargetRow(
                    "es-order",
                    primaryKeys,
                    row.getColumns(),
                    row.getRowKind());
        };
    }
}