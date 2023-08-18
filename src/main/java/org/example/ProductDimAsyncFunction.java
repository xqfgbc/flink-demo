package org.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ProductDimAsyncFunction extends RichAsyncFunction<SourceRow, SourceRow> {

    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;

    private String tableName;

    public ProductDimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/mydb", "root", "root");

        threadPoolExecutor = new ThreadPoolExecutor(8,
                16,
                1L,
                TimeUnit.MINUTES,
                new LinkedBlockingDeque<>());
    }

    @Override
    public void asyncInvoke(SourceRow sourceRow, ResultFuture<SourceRow> resultFuture) {
        threadPoolExecutor.submit(() -> {
            String id = getKey(sourceRow);
            String querySql = "select * from " + tableName + " where id ='" + id + "'";
            List<JSONObject> queryList = null;
            try {
                queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            JSONObject dimInfo = queryList.get(0);
            if (dimInfo != null) {
                assembleDimValue(sourceRow, dimInfo);
            }
            resultFuture.complete(Collections.singletonList(sourceRow));
        });
    }

    private void assembleDimValue(SourceRow sourceRow, JSONObject dimInfo) {
        sourceRow.getColumns().put("product_name", dimInfo.getString("name"));
        sourceRow.getColumns().put("product_description", dimInfo.getString("description"));
    }

    protected String getKey(SourceRow sourceRow) {
        return sourceRow.getColumns().get("product_id");
    }

    @Override
    public void timeout(SourceRow input, ResultFuture<SourceRow> resultFuture) {
        System.out.println("TimeOut：" + input);
    }
}
