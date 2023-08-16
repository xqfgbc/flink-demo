# flink cdc datastream demo

## 准备工作（flink基于1.17.0版本）
* 本demo默认你已经完成了flink cdc官方的demo，在本地有mysql，flink，elastic-search的环境 [quick start](https://ververica.github.io/flink-cdc-connectors/master/content/%E5%BF%AB%E9%80%9F%E4%B8%8A%E6%89%8B/mysql-postgres-tutorial-zh.html)
* 受环境限制，以下提供本地podman容器单独启动各组件的方式
1. 启动相关镜像
```shell
podman network create esnetwork
podman run -d --name elasticsearch --net esnetwork -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" -e "cluster.name=docker-cluster" -e "bootstrap.memory_lock=true" -e "ES_JAVA_OPTS=-Xms128m -Xmx128m" elasticsearch:7.17.10
podman run -d --name kibana --net esnetwork -p 5601:5601 kibana:7.17.10

// mysql的特殊镜像，已开启binlog设置
podman run -d --rm --name mysql --net esnetwork -p 3306:3306 -e MYSQL_ROOT_PASSWORD=root -e MYSQL_USER=mysqluser -e MYSQL_PASSWORD=mysqlpw quay.io/debezium/example-mysql:2.3
```
2. 在本地使用flink sql时可能遇到时区问题（flink在本地，mysql数据库在容器中），可以采用以下方式增加时区设置
```sql
CREATE TABLE orders (
   order_id INT,
   order_date TIMESTAMP(0),
   customer_name STRING,
   price DECIMAL(10, 5),
   product_id INT,
   order_status BOOLEAN,
   PRIMARY KEY (order_id) NOT ENFORCED
 ) WITH (
  'server-time-zone'='UTC',
   'connector' = 'mysql-cdc',
   'hostname' = '127.0.0.1',
   'port' = '3306',
   'username' = 'root',
   'password' = 'root',
   'database-name' = 'mydb',
   'table-name' = 'orders'
 );
```

3. flink与flink cdc的版本对应关系有强一致性要求，参考build.gradle中的配置

## 本demo要解决的问题
* 在ide中，使用flink datastream api方式获取mysql数据库变化
* 在ide中，使用jdbc的方式从source库查询关联表数据
* 在ide中，将之前步骤数据进行聚合处理并写入es
* 将demo发布到flink集群中运行