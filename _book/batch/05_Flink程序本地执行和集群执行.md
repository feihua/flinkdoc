# Flink批处理

## 1. Flink程序本地执行和集群执行

### 1.1. 本地执行

Flink支持两种不同的本地执行。 

`LocalExecutionEnvironment` 是启动完整的Flink运行时（Flink Runtime），包括 JobManager 和 TaskManager 。 这种方式包括内存管理和在集群模式下执行的所有内部算法。

`CollectionEnvironment` 是在 Java 集合（Java Collections）上执行 Flink 程序。 此模式不会启动完整的Flink运行时（Flink Runtime），因此执行的开销非常低并且轻量化。 例如一个`DataSet.map()`变换，会对Java list中所有元素应用 `map()` 函数。

#### 1.1.1. local环境

`LocalEnvironment`是Flink程序本地执行的句柄。可使用它，独立或嵌入其他程序在本地 JVM 中运行Flink程序。

本地环境通过该方法实例化`ExecutionEnvironment.createLocalEnvironment()`。

 默认情况下，启动的`本地线程数`与计算机的`CPU个数`相同。也可以指定所需的并行性。本地环境可以配置为使用enableLogging()/ 登录到控制台disableLogging()。

在大多数情况下，ExecutionEnvironment.getExecutionEnvironment()是更好的方式。LocalEnvironment当程序在本地启动时（命令行界面外），该方法会返回一个程序，并且当程序由命令行界面调用时，它会返回一个预配置的群集执行环境。

>    注意：本地执行环境不启动任何Web前端来监视执行。

```scala
/**
  * local环境
  */
object BatchCollectionsEven {
  def main(args: Array[String]): Unit = {
    // 开始时间
    var start_time =new Date().getTime
    //TODO 初始化本地执行环境
    val env = ExecutionEnvironment.createLocalEnvironment
    val list: DataSet[String] = env.fromCollection(List("1","2"))
      
    list.print()

    // 结束时间
    var end_time =new Date().getTime
    println(end_time-start_time) //单位毫秒
  }
}
```



#### 6.1.2. 集合环境

使用集合的执行CollectionEnvironment是执行Flink程序的低开销方法。这种模式的典型用例是自动化测试，调试和代码重用。

用户也可以使用为批处理实施的算法，以便更具交互性的案例

>   请注意，基于集合的Flink程序的执行仅适用于适合JVM堆的小数据。集合上的执行不是多线程的，只使用一个线程

```scala
/**
  * local环境
  */
object BatchCollectionsEven {
  def main(args: Array[String]): Unit = {
    // 开始时间
    var start_time =new Date().getTime
    //TODO 初始化本地执行环境
    val env = ExecutionEnvironment.createCollectionsEnvironment
    val list: DataSet[String] = env.fromCollection(List("1","2"))
      
    list.print()

    // 结束时间
    var end_time =new Date().getTime
    println(end_time-start_time) //单位毫秒
  }
}
```



### 1.2. 集群执行

Flink程序可以在许多机器的集群上分布运行。有两种方法可将程序发送到群集以供执行：

-   使用命令行界面提交
-   使用代码中的远程环境提交

#### 1.2.1. 使用命令行提交

```shell
./bin/flink run ./examples/batch/WordCount.jar   --input file:///home/user/hamlet.txt --output file:///home/user/wordcount_out
```



#### 1.2.2. 使用代码中远程环境提交

通过IDE,直接在远程环境上执行Flink Java程序。



**操作步骤**

1. 添加Maven插件

   ```scala
   <build>
       <plugins>
           <plugin>
               <groupId>org.apache.maven.plugins</groupId>
               <artifactId>maven-jar-plugin</artifactId>
               <version>2.6</version>
               <configuration>
                   <archive>
                       <manifest>
                           <addClasspath>true</addClasspath>
                           <classpathPrefix>lib/</classpathPrefix>
                           <mainClass>com.flink.DataStream.RemoteEven</mainClass>
                       </manifest>
                   </archive>
               </configuration>
           </plugin>
           <plugin>
               <groupId>org.apache.maven.plugins</groupId>
               <artifactId>maven-dependency-plugin</artifactId>
               <version>2.10</version>
               <executions>
                   <execution>
                       <id>copy-dependencies</id>
                       <phase>package</phase>
                       <goals>
                           <goal>copy-dependencies</goal>
                       </goals>
                       <configuration>
                           <outputDirectory>${project.build.directory}/lib</outputDirectory>
                       </configuration>
                   </execution>
               </executions>
           </plugin>
       </plugins>
   </build>
   ```

   

   

   ```java
   /**
    创建远程执行环境。远程环境将程序（部分）发送到集群以执行。请注意，程序中使用的所有文件路径都必须可以从集群中访问。除非通过[[ExecutionEnvironment.setParallelism（）]显式设置并行度，否则执行将使用集群的默认并行度。 
   
    * @param host  JobManager的ip或域名
    * @param port  JobManager的端口
    * @param jarFiles 包含需要发送到集群的代码的JAR文件。如果程序使用用户定义的函数、用户定义的输入格式或任何库，则必须在JAR文件中提供这些函数。
    */
   def createRemoteEnvironment(host: String, port: Int, jarFiles: String*): ExecutionEnvironment = {
       new ExecutionEnvironment(JavaEnv.createRemoteEnvironment(host, port, jarFiles: _*))
   }
   ```

   

**示例**

读取HDFS上的score.csv文件, 获取到每个学生最好的成绩信息.



**开发步骤**

1. 创建远程执行环境
2. 读取远程CSV文件,转换成元组类型
3. 根据姓名分组,按成绩倒序排列,取第一个值
4. 打印结果



**代码**

```scala
object BatchRemoteEven {

  def main(args: Array[String]): Unit = {
    // 1. 创建远程执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.createRemoteEnvironment("node01", 8081, "E:\\bigdata_ws\\flink-base\\target\\flink-base-1.0-SNAPSHOT.jar")
    // 2. 读取远程CSV文件,转换为元组类型
    val scoreDatSet: DataSet[(Long, String, Long, Double)] = env.readCsvFile[(Long, String, Long, Double)]("hdfs://node01:8020/flink-datas/score.csv")
    // 3. 根据姓名分组,按成绩倒序排列,取第一个值
    val list: DataSet[(Long, String, Long, Double)] = scoreDatSet.groupBy(1).sortGroup(3,Order.DESCENDING).first(1)
    // 4. 打印结果
    list.print()
  }
}
```


