import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.catalyst.optimizer.LikeSimplification;
import scala.Tuple2;

import javax.security.auth.login.Configuration;
import java.io.IOException;
import java.sql.Connection;
import java.util.Arrays;
import java.lang.Iterable;
import java.util.Iterator;
import java.util.List;

/**
 * Created by liufukai on 2018/9/26.
 */
public class SparkWordCount {

    //与HBase数据库的连接对象
    public static org.apache.hadoop.hbase.client.Connection connection;
    //数据库元数据操作对象
    public static Admin admin;

    public static void main(String[] args){
        try{
            System.out.println("开始执行本程序");
            System.out.println("HBaseDemo.main()->admin1:"+admin);
            setUp();
            //这里调用下面的方法来操作hbase

        } catch (Exception e){
            e.printStackTrace();
        }
        /**
         * 第一步：创建SparkConf
         * setMaster 设置集群的 master的url，如果设置为local，表示在本地运行
         * 如果提交集群，要删除setmaster
         */
        SparkConf sparkconf=new SparkConf().setAppName("WordCountCluster");//.setMaster("local");
        /**
         * 第二步：创建SparkContext对象
         * 在spark中，SparkContext 是spark 所有功能的入口 ，无论使用的是java scala 甚至 py ，编写都必须有一个SparkContext
         * 它的主要作用包括初始化spark 应用程序所需要的一些核心组件，包括调度器（DAGSchedule,taskScheduler），他还会去spark master 节点上去注册等等
         * 但是呢，在spark中编写不同类型的spark应用程序，使用的sparkContext
         * 如果使用scala 使用原生的SparkContext
         * 如果使用java 那么就是使用JavaSparkContext
         * 如果使用Spark Sql 程序， 那么就是 SQLContext，HiveContext
         * 如果开发Spark Streaming 程序，那么就是它独有的SparkContext
         * 以此类推
         */
        JavaSparkContext sc=new JavaSparkContext(sparkconf);
        /**
         * 第三步：要针对输入源（hdfs文件，本地文件，等等）创建一个初始的RDD
         * 输入源中的数据会被打散，分配到RDD的每个partition 中从而形成一个初始的分布式程序集
         * 本次测试 所以针对本地文件
         * SparkContext 中用于根据文件类型的输入源创建 RDD 的方法，叫做textFile()
         * 在我们这里呢，RDD 中有元素这种概念，如果是 hdfs 或者本地文件呢，创建RDD 每一个文件就相当于文件里的一行
         */
        JavaRDD<String> lines = sc.textFile(args[0], 1);
        //JavaRDD<String> lines=sc.textFile("/Users/liufukai/Desktop/aow_drv.txt");
        /**
         * 第四步：对初始RDD进行transformation操作，也就是一些计算操作
         * 通常操作会通过创建function，并配合RDD的map、flatMap等算子来执行
         * function，通常，如果比较简单，则创建指定 Function的匿名内部类
         * 但是如果function比较复杂，则会单独创建一个类，作为实现这个function接口的类
         * 先将每一行拆分成单个的单词
         * FlatMapFunction，有两个泛型参数，分别代表了输入和输出类型
         * 我们这里呢，输入肯定是String，因为是一行一行的文本，输出，其实也是String，因为是每一行的文本
         * 这里先简要介绍flatMap算子的作用，其实就是，将RDD的一个元素，给拆分成一个或多个元素
         */
        JavaRDD<String> words=lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID=1L;//用来表明类的不同版本间的兼容性
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        /**
         * 接着，需要将每一个单词，映射为（单词，1）的这种格式
         * 因为只有这样，后面才能根据单词作为key，来进行每个单词的出现次数的累加
         * mapToPair，其实就是将每个元素，映射为一个（v1，v2）这样的Tuple2类型的元素
         * 如果大家还记得scala里面讲的tuple，那么没错，这里的tuple2就是scala类型，包含来两个值
         * mapToPair这个算子，要求的是与PairFunction配合使用，第一个泛型参数代表来输入类型
         * 第二个和第三个泛型参数，代表的输出的Tuple2的第一个值和第二个值的类型
         * JavaPairRDD的两个泛型参数，分别代表了tuple元素的第一个值和第二个值的类型
         */
        JavaPairRDD<String,Integer> pairs=words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    private static final long serialVersionUID=1L;
                    @Override
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word,1);
                    }
                }
        );
        /**
         * 接着，需要以单词作为key，统计每个单词出现的次数
         * 这里要使用reduceByKey这个算子，对每个key对应的value，都进行reduce操作
         * 比如 JavaPairRDD中有几个元素，分别为（hello，1）（hello，1）（hello，1）（world，1）
         * reduce操作，相当于是把第一个值和第二个值进行计算，然后再将结果与第三个值进行计算
         * 比如这里的hello，那么就相当于是，首先是1+1=2 ， 然后再将2+1=3
         * 最后返回的JavaPairRDD 中的元素，也是tuple，但是第一个值就是每个key，第二个值就是key的value
         * reduce之后的结果，相当于就是每个单词出现的次数
         */
        JavaPairRDD<String,Integer> wordCounts=pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1+v2;
                    }
                }
        );
        /**
         * 到这里为止，我们通过几个Spark算子操作，已经统计出了单词的次数
         * 但是，之前我们使用的flatMap、mapToPair、reduceByKey这种操作，都叫做transformation
         * 一个Spark应用只能够，光是有transformation操作，是不行的，是不会执行的，必须要有一种叫做action
         * 接着，最后，可以使用一种叫做action操作的，比如是，foreach，来触发程序的执行
         */
        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID=1L;
            @Override
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1+":"+wordCount._2);
            }
        });

        sc.close();
    }

    public static void setUp() throws Exception{
        //取得一个数据库配置参数对象
        org.apache.hadoop.conf.Configuration configuration= HBaseConfiguration.create();
        //设置连接参数：hbase数据库所在的主机IP
        configuration.set("hbase.zookeeper.quorum","master,slave1,slave2");
        //设置连接参数：hbase数据库使用的接口
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        configuration.set("hbase.master","172.16.197.162:16000");
        //取得一个数据库连接对象
        connection= ConnectionFactory.createConnection(configuration);
        //取得一个数据库元数据操作对象
        admin=connection.getAdmin();
        System.out.println("HBaseDemo.setUp()->admin:"+admin);
    }

    /**
     * 创建表的方法，输入参数为表名
     */
    public static void createTable(String tableNameString) throws IOException{
        System.out.println("------------开始创建表-------------");
        //新建一个数据表表名对象
        TableName tableName= TableName.valueOf(tableNameString);
        System.out.println("HBaseDemo.createTable()->tableName:"+tableName);
        if(admin.tableExists(tableName)){
            System.out.println("表已经存在了！");
        }
        else{
            //数据表描述对象
            HTableDescriptor hTableDescriptor=new HTableDescriptor(tableName);
            System.out.println("HBaseDemo.createTable()->hTableDescriptor:"+hTableDescriptor);
            //数据簇描述对象
            HColumnDescriptor family=new HColumnDescriptor("hase");
            System.out.println("HBaseDemo.createTable()->family:"+family);
            //在数据表中新建一个列簇
            hTableDescriptor.addFamily(family);
            //新建数据表
            admin.createTable(hTableDescriptor);
            System.out.println("HBaseDemo.createTable()->admin3:"+admin);
        }
        System.out.println("----------创建表结束-------------");
    }
    /**
     * 查询表中的数据
     */
    public static void queryTable(String tableNameString) throws IOException{
        System.out.println("-----------查询整表的数据-----------");
        //获取数据表对象
        Table table=connection.getTable(TableName.valueOf(tableNameString));
        //获取表中的数据
        ResultScanner scanner=table.getScanner(new Scan());
        //循环输出表中的数据
        for(Result result:scanner){
            byte[] row=result.getRow();
            System.out.println("row key is:"+new String(row));
            List<Cell> listCells=result.listCells();
            for (Cell cell:listCells){
                byte[] familyArray=cell.getFamilyArray();
                byte[] qualifierArray=cell.getQualifierArray();
                byte[] valueArray=cell.getValueArray();
                System.out.println("row value is:"+new String(familyArray)+new String(qualifierArray)+new String(valueArray));
            }
        }
        System.out.println("-----------查询整表数据结束-------------");
    }

}
