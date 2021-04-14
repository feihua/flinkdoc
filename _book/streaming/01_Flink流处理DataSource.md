# Flink流处理

## 1. 输入数据集DataSource

Flink 中你可以使用 `StreamExecutionEnvironment.getExecutionEnvironment`创建流处理的执行环境

Flink 中你可以使用 StreamExecutionEnvironment.addSource(source) 来为你的程序添加数据来源。

Flink 已经提供了若干实现好了的 source functions，当然你也可以通过实现 `SourceFunction`来自定义非并行的source或者实现 `ParallelSourceFunction` 接口或者扩展 `RichParallelSourceFunction` 来自定义并行的 source。

Flink在流处理上的source和在批处理上的source基本一致。大致有4大类：

*   基于本地集合的source（Collection-based-source）

*   基于文件的source（File-based-source）- 读取文本文件，即符合 TextInputFormat 规范的文件，并将其作为字符串返回

*   基于网络套接字的source（Socket-based-source）- 从 socket 读取。元素可以用分隔符切分。

*   自定义的source（Custom-source）

### 1.1. 基于集合的source

>   //创建流处理的执行环境
>
>   val env = StreamExecutionEnvironment.getExecutionEnvironment
>
>   //使用env.fromElements()来创建数据源
>
>   val dataStream: DataStream[String] = env.fromElements("spark", "flink")

```scala
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import scala.collection.immutable.{Queue, Stack}
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object StreamingDemoFromCollectionSource {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    //0.用element创建DataStream(fromElements)
    val ds0: DataStream[String] = senv.fromElements("spark", "flink")
    ds0.print()

    //1.用Tuple创建DataStream(fromElements)
    val ds1: DataStream[(Int, String)] = senv.fromElements((1, "spark"), (2, "flink"))
    ds1.print()

    //2.用Array创建DataStream
    val ds2: DataStream[String] = senv.fromCollection(Array("spark", "flink"))
    ds2.print()

    //3.用ArrayBuffer创建DataStream
    val ds3: DataStream[String] = senv.fromCollection(ArrayBuffer("spark", "flink"))
    ds3.print()

    //4.用List创建DataStream
    val ds4: DataStream[String] = senv.fromCollection(List("spark", "flink"))
    ds4.print()

    //5.用List创建DataStream
    val ds5: DataStream[String] = senv.fromCollection(ListBuffer("spark", "flink"))
    ds5.print()

    //6.用Vector创建DataStream
    val ds6: DataStream[String] = senv.fromCollection(Vector("spark", "flink"))
    ds6.print()

    //7.用Queue创建DataStream
    val ds7: DataStream[String] = senv.fromCollection(Queue("spark", "flink"))
    ds7.print()

    //8.用Stack创建DataStream
    val ds8: DataStream[String] = senv.fromCollection(Stack("spark", "flink"))
    ds8.print()

    //9.用Stream创建DataStream（Stream相当于lazy List，避免在中间过程中生成不必要的集合）
    val ds9: DataStream[String] = senv.fromCollection(Stream("spark", "flink"))
    ds9.print()

    //10.用Seq创建DataStream
    val ds10: DataStream[String] = senv.fromCollection(Seq("spark", "flink"))
    ds10.print()

    //11.用Set创建DataStream(不支持)
    //val ds11: DataStream[String] = senv.fromCollection(Set("spark", "flink"))
    //ds11.print()

    //12.用Iterable创建DataStream(不支持)
    //val ds12: DataStream[String] = senv.fromCollection(Iterable("spark", "flink"))
    //ds12.print()

    //13.用ArraySeq创建DataStream
    val ds13: DataStream[String] = senv.fromCollection(mutable.ArraySeq("spark", "flink"))
    ds13.print()

    //14.用ArrayStack创建DataStream
    val ds14: DataStream[String] = senv.fromCollection(mutable.ArrayStack("spark", "flink"))
    ds14.print()

    //15.用Map创建DataStream(不支持)
    //val ds15: DataStream[(Int, String)] = senv.fromCollection(Map(1 -> "spark", 2 -> "flink"))
    //ds15.print()

    //16.用Range创建DataStream
    val ds16: DataStream[Int] = senv.fromCollection(Range(1, 9))
    ds16.print()

    //17.用fromElements创建DataStream
    val ds17: DataStream[Long] = senv.generateSequence(1, 9)
    ds17.print()

    senv.execute(this.getClass.getName)
  }
}

```

### 1.2. 基于文件的source

Flink的流处理可以直接通过`readTextFile()`方法读取文件来创建数据源,方法如下:

```scala
object DataSource_CSV {
  def main(args: Array[String]): Unit = {
    // 1. 获取流处理运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2. 读取文件
    val textDataStream: DataStream[String] = env.readTextFile("hdfs://node01:8020/flink-datas/score.csv")
    // 3. 打印数据
    textDataStream.print()
    // 4. 执行程序
    env.execute()
  }
}
```



### 1.3. 基于网络套接字的source

上面两种方式创建的数据源一般都是固定的.如果需要源源不断的产生数据,可以使用socket的方式来获取数据,通过调用`socketTextStream()`方法

**示例**

编写Flink程序，接收`socket`的单词数据，并以空格进行单词拆分打印。



**步骤**

1. 获取流处理运行环境

2. 构建socket流数据源，并指定IP地址和端口号

3. 对接收到的数据进行空格拆分

4. 打印输出

5. 启动执行

6. 在Linux中，使用`nc -lk 端口号`监听端口，并发送单词

   > **安装nc: ** yum install -y nc 
   >
   > nc -lk 9999 监听9999端口的信息



**代码**

```scala
object SocketSource {
  def main(args: Array[String]): Unit = {
    //1. 获取流处理运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //  2. 构建socket流数据源，并指定IP地址和端口号
    // hadoop hadoop hive spark
    val socketDataStream: DataStream[String] = env.socketTextStream("node01", 9999)
    //  3. 转换,以空格拆分单词
    val mapDataSet: DataStream[String] = socketDataStream.flatMap(_.split(" "))
    //  4. 打印输出
    mapDataSet.print()
    //  5. 启动执行
    env.execute("WordCount_Stream")
  }
}
```

### 1.4. 自定义source

我们也可以通过去实现`SourceFunction`或者它的子类`RichSourceFunction`类来自定义实现一些自定义的source，Kafka创建source数据源类`FlinkKafkaConsumer010`也是采用类似的方式。



#### 1.4.1. 自定义数据源



**示例:**

自定义数据源, 每1秒钟随机生成一条订单信息(`订单ID`、`用户ID`、`订单金额`、`时间戳`)

要求: 

- 随机生成订单ID（UUID）

- 随机生成用户ID（0-2）

- 随机生成订单金额（0-100）

- 时间戳为当前系统时间

  

**开发步骤:**

1.  创建订单样例类

2. 获取流处理环境

3. 创建自定义数据源

   - 循环1000次
   - 随机构建订单信息
   - 上下文收集数据
   - 每隔一秒执行一次循环

4. 打印数据

5. 执行任务

   



**代码:**

```scala
object StreamFlinkSqlDemo {

  // 创建一个订单样例类Order，包含四个字段（订单ID、用户ID、订单金额、时间戳）
  case class Order(id: String, userId: Int, money: Long, createTime: Long)

  def main(args: Array[String]): Unit = {
    // 1. 获取流处理运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2. 创建一个自定义数据源
    val orderDataStream = env.addSource(new RichSourceFunction[Order] {
      override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
        // 使用for循环生成1000个订单
        for (i <- 0 until 1000) {
          // 随机生成订单ID（UUID）
          val id = UUID.randomUUID().toString
          // 随机生成用户ID（0-2）
          val userId = Random.nextInt(3)
          // 随机生成订单金额（0-100）
          val money = Random.nextInt(101)
          // 时间戳为当前系统时间
          val timestamp = System.currentTimeMillis()
          // 收集数据
          ctx.collect(Order(id, userId, money, timestamp))
          // 每隔1秒生成一个订单
          TimeUnit.SECONDS.sleep(1)
        }
      }

      override def cancel(): Unit = ()
    })
    // 3. 打印数据
    orderDataStream.print()
    // 4. 执行程序
    env.execute()
  }
}
```



#### 1.4.2. 使用Kafka作为数据源

我们可以通过使用`FlinkKafkaConsumer010`来从Kafka中获取消息:



**示例:**

使用Flink流处理方式,读取Kafka的数据,并打印.

**开发步骤:**

1. 创建流处理环境
2. 指定链接kafka相关信息
3. 创建kafka数据流(FlinkKafkaConsumer010)
4. 添加Kafka数据源
5. 打印数据
6. 执行任务



> **Kafka相关操作: **
>
> **创建topic**
>
> kafka-topics.sh --create --partitions 3 --replication-factor 2 --topic kafkatopic --zookeeper node01:2181,node02:2181,node03:2181
>
> **模拟生产者**
>
> kafka-console-producer.sh --broker-list node01:9092,node02:9092,node03:9092 --topic kafkatopic
>
> **模拟消费者**
>
> kafka-console-consumer.sh --from-beginning --topic kafkatopic --zookeeper node01:2181,node02:2181,node03:2181



**代码:**

```scala
import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.kafka.clients.CommonClientConfigs

object DataSource_kafka {
  def main(args: Array[String]): Unit = {

    //1. 创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
      
    //2. 指定kafka数据流的相关信息
    val kafkaCluster = "node01:9092,node02:9092,node03:9092"
    val kafkaTopicName = "kafkatopic"

    //3. 创建kafka数据流
    val properties = new Properties()
    properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster)
    val kafka010 = new FlinkKafkaConsumer010[String](kafkaTopicName, new SimpleStringSchema(), properties)

    //4. 添加数据源addSource(kafka010)
    val text: DataStream[String] = env.addSource(kafka010)

    //5. 打印数据
    text.print()

    //6. 执行任务
    env.execute("flink-kafka-wordcunt")
  }
}
```



#### 1.4.3. 使用MySQL作为数据源

上面我们已经使用了自定义数据源和Flink自带的Kafka source，那么接下来就模仿着写一个从 MySQL 中读取数据的 Source。



**示例**

自定义数据源, 读取MySql数据库(test)表(user)数据.

| id   | username | password | name |
| ---- | -------- | -------- | ---- |
| 1    | zhangsan | 111111   | 张三 |
| 2    | lisi     | 222222   | 李四 |
| 3    | wangwu   | 333333   | 王五 |
| 4    | zhaoliu  | 444444   | 赵六 |
| 5    | tianqi   | 555555   | 田七 |



**相关依赖**

```xml
 <!-- 指定mysql-connector的依赖 -->
 <dependency>
     <groupId>mysql</groupId>
     <artifactId>mysql-connector-java</artifactId>
     <version>5.1.38</version>
 </dependency>
```



**开发步骤**

1. 自定义Source,继承自RichSourceFunction
2. 实现run方法
   1. 加载驱动
   2. 创建连接
   3. 创建PreparedStatement
   4. 执行查询
   5. 遍历查询结果,收集数据
3. 使用自定义Source
4. 打印结果
5. 执行任务

**代码**

```scala
package com.itheima.stream

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object DataSource_mysql {
  def main(args: Array[String]): Unit = {
    // 1. 创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2. 设置并行度
    env.setParallelism(1)
    // 3. 添加自定义MySql数据源
    val source = env.addSource(new MySql_source)
    // 4. 打印结果
    source.print()
    // 5. 执行任务
    env.execute()
  }
}

class MySql_source extends RichSourceFunction[(Int, String, String, String)] {

  override def run(ctx: SourceContext[(Int, String, String, String)]): Unit = {

    // 1. 加载MySql驱动
    Class.forName("com.mysql.jdbc.Driver")
    // 2. 链接MySql
    var connection: Connection = DriverManager.getConnection("jdbc:mysql:///test", "root", "123456")
    // 3. 创建PreparedStatement
    val sql = "select id , username , password , name from user"
    var ps: PreparedStatement = connection.prepareStatement(sql)

    // 4. 执行Sql查询
    val queryRequest = ps.executeQuery()
    // 5. 遍历结果
    while (queryRequest.next()) {
      val id = queryRequest.getInt("id")
      val username = queryRequest.getString("username")
      val password = queryRequest.getString("password")
      val name = queryRequest.getString("name")
      // 收集数据
      ctx.collect((id, username, password, name))
    }
  }

  override def cancel(): Unit = {}
}
```

