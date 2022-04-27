# 流数据部分

## 1. 实验内容

1. 使用kafka接受助教数据，共得到17.37GB数据，时间跨度从2021-1-1到2022-3-8，经处理后数据以每行一个JSON的格式存储在本地的txt文件“bank2.txt”中，具体如下：

   ![2022-04-26 22-34-35 的屏幕截图](https://raw.githubusercontent.com/Leonardo-hf/DI-Homework-stage-2/master/assets/2022-04-26 22-34-35 的屏幕截图.png)

2. 使用kafka创建topic并发送数据。

3. 使用flink以kafka数据为数据源，使用map将文本信息变换成JSON对象，再根据eventType分流，将不同类型的数据下沉到clickhouse的不同表中。

4. 可视化部分：以shop表为例，使用echart.js编写折线图表，描述每日订单数量（及销售额）变化，并提供不同时间粒度下的呈现形式，通过每秒向后端发送请求获取最新数据。

5. 项目代码：https://github.com/Leonardo-hf/DI-Homework-stage-2

## 2. 详细设计

### 2.1 kafka发送数据

```java
public class Producer {

    // 定义TOPIC
    public final static String TOPIC = "bank";

    // 对发送数据条目进行计数，验证发送方和接收方数据的一致性，保证没有重复消费或漏消费
    public static int cal = 0;

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        // 删除TOPIC及TOPIC下之前生产的log
        AdminClient kafkaAdminClient = KafkaAdminClient.create(props);
        List<String> topics = new ArrayList<>();
        topics.add(TOPIC);
        kafkaAdminClient.deleteTopics(topics);

        // 设置生产者属性
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("group.id", "191250048");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        // 从文本读取数据
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream("/home/approdite/IdeaProjects/kafka/bank2.txt"));
        int bufferSize = 10 * 1024 * 1024;
        BufferedReader in = new BufferedReader(new InputStreamReader(bis), bufferSize);
        // 设置log线程，每两秒输出当前生产数据的条目信息
        Thread log = new Thread(() -> {
            int times = 0, last = 0;
            while (true) {
                if (last == cal) {
                    times++;
                    // 连续5次，生产数据的条目没有变化则认为生产结束，log线程停止
                    if (times >= 5) {
                        break;
                    }
                } else {
                    last = cal;
                    times = 0;
                }
                System.err.println("produce message :" + last);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.err.println("retry over 5 times, producing could be finished!");
        });
        log.start();
        // 以行为单位读取并生产数据
        String line;
        while ((line = in.readLine()) != null && !line.isEmpty()) {
            ++cal;
            producer.send(new ProducerRecord<>(TOPIC, line));
        }
        in.close();
        producer.close();
        kafkaAdminClient.close();
    }
}
```

### 2.2 flink处理数据及数据下沉

#### 2.2.1 flink处理数据部分

>flink以kafka Consumer为数据源，经过etl处理后下沉到clickhouse；

```java
public class Consumer {

// 	  // 使用OutputTag将可视化需要的shop做额外的侧输出，用于生成可视化需要的数据
//    public static OutputTag<v_tr_shop_mx> statistic = new OutputTag<v_tr_shop_mx>("id") {
//    };
//	  // 为OutputTag添置数据
//    public static final class Processor extends ProcessFunction<Insert, Insert> {
//
//        @Override
//        public void processElement(Insert insert, Context context, Collector<Insert> collector) {
//            context.output(statistic, (v_tr_shop_mx) insert);
//            collector.collect(insert);
//        }
//    }

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.enableCheckpointing(5000);
        // 设置消费者，从最早开始拉取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                Producer.TOPIC,
                new SimpleStringSchema(),
                properties);
//        consumer.setStartFromGroupOffsets();
        consumer.setStartFromEarliest();
        final String KEY = "eventType";
        final String BODY = "eventBody";
        // 将数据源设置为kafka消费者
        DataStreamSource<String> sourceStream = env.addSource(consumer).setParallelism(1);
        // 将接受的数据转换为json对象
        SingleOutputStreamOperator<JSONObject> jsonObjectStream = sourceStream.map(JSONObject::parseObject);
        // 表驱动，根据eventType将不同的json对象转化为对应的java类
        String[] keys = new String[]{"shop", "sjyh", "sdrq", "sbyb", "sa", "huanx", "huanb", "gzdf", "grwy", "etc", "duebill", "dsf", "djk", "contract"};
        Class<Insert>[] classes = new Class[]{v_tr_shop_mx.class, dm_v_tr_sjyh_mx.class, dm_v_tr_sdrq_mx.class, dm_v_tr_sbyb_mx.class, dm_v_tr_sa_mx.class, dm_v_tr_huanx_mx.class, dm_v_tr_huanb_mx.class, dm_v_tr_gzdf_mx.class, dm_v_tr_grwy_mx.class, dm_v_tr_etc_mx.class, dm_v_tr_duebill_mx.class, dm_v_tr_dsf_mx.class, dm_v_tr_djk_mx.class, dm_v_tr_contract_mx.class};
        // 根据eventType分流，给不同的java设置自身的sink方法
        for (int i = 0; i < keys.length; i++) {
            int finalI = i;
            SingleOutputStreamOperator<Insert> pojoStream = jsonObjectStream
                    .filter(e -> {
                        try {
                            return e.get(KEY).equals(keys[finalI]);
                        } catch (Exception s) {
                            System.err.println(e);
                            throw s;
                        }
                    })
                    .map(jsonObject ->
                    {
                        try {
                            return jsonObject.getJSONObject(BODY).toJavaObject(classes[finalI]);
                        } catch (JSONException e) {
                            System.err.println(jsonObject);
                            throw e;
                        }
                    });
            pojoStream.addSink(new POJOInsertSink());
        }
//		  // 使用OutputTag分流shop流，设置额外的sink方法，为可视化提供快速查询需要的数据
//        SingleOutputStreamOperator<Insert> pojoStream = jsonObjectStream
//                .filter(e -> e.get(KEY).equals("shop"))
//                .map(jsonObject -> (Insert) jsonObject.getJSONObject(BODY).toJavaObject(v_tr_shop_mx.class)).process(new Processor());
//        DataStream<v_tr_shop_mx> output = pojoStream.getSideOutput(statistic);
//        output.addSink(new StatisticSink());
//        pojoStream.addSink(new POJOInsertSink());
        try {
            env.execute("consume start");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```



#### 2.2.2 sink数据下沉部分

> 这里有些偷懒，所有POJO在sink中仅有insert这一种下沉方式，故使所有POJO实现Insert接口，Insert接口要求POJO自身提供insert相关的PreparedStatement，并且依据自身属性为PreparedStatement插入数据。POJOInsertSink类仅调用POJO的Statement，并提供批量插入的功能。

```java
public class POJOInsertSink extends RichSinkFunction<Insert> {

    // 数据库连接
    Connection connection = null;

    // 批量插入的下限，不应超过生产数据的批量（16384），
    // 设置较大时，插入效率更高，但插入频率降低不利于可视化，在可视化展示时可合理降低standard值，建议为512
    final int standard = 5120;

    // 为每种POJO维护自己的PreparedStatement，事实上每个POJO都有自己的Sink，所以不会获得其他POJO的PreparedStatement，故使用Map没有意义，可删去。
    Map<Class<?>, PreparedStatement> stats = new ConcurrentHashMap<>();
    
    // 同上，为每种POJO维护自己的PreparedStatement已经加入Batch的条目。
    Map<Class<?>, Integer> count = new ConcurrentHashMap<>();

    // 连接到clickhouse数据库，取消自动提交
    @Override
    public void open(Configuration parameters) {
        try {
            super.open(parameters);
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            // compress 默认为 true，但当前版本的clickhouse对compress支持不佳可能会报错，这里设置为false
            // String url = "jdbc:clickhouse://localhost:8123/dm?compress=false";
            String url = "jdbc:clickhouse://124.222.139.8:8123/dm?compress=false";
            connection = DriverManager.getConnection(url);
            connection.setAutoCommit(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 关闭数据库连接，但是此时可能还存在PreparedStatement没有达到批量插入的standard，此时也将其执行
    @Override
    public void close() throws Exception {
        for (Map.Entry<Class<?>, PreparedStatement> entry : stats.entrySet()) {
            if (count.get(entry.getKey()) > 0) {
                long startTime = System.currentTimeMillis();
                int size = entry.getValue().executeBatch().length;
                connection.commit();
                long endTime = System.currentTimeMillis();
//                System.out.println("批量插入完毕用时：" + (endTime - startTime) + " -- 插入数据(" + entry.getKey().getSimpleName() + ") = " + size);
                entry.getValue().clearBatch();
            }
        }
        super.close();
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void invoke(Insert pojo, Context context) {
        try {
            // 获得POJO对应的PreparedStatement和已准备条目
            Class<?> pclass = pojo.getClass();
            stats.putIfAbsent(pclass, pojo.stat(connection));
            PreparedStatement preparedStatement = stats.get(pclass);
            pojo.insert(preparedStatement);
            preparedStatement.addBatch();
            count.putIfAbsent(pclass, 0);
            int cnt = count.get(pclass) + 1;
            // 当条目数大于等于standard，则批量执行插入语句
            if (cnt >= standard) {
                long startTime = System.currentTimeMillis();
                int size = preparedStatement.executeBatch().length;
                connection.commit();
                long endTime = System.currentTimeMillis();
//                System.out.println("批量插入完毕用时：" + (endTime - startTime) + " -- 插入数据(" + pclass.getSimpleName() + ") = " + size);
                preparedStatement.clearBatch();
                cnt = 0;
            }
            count.put(pclass, cnt);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
```

#### 2.2.3 POJO部分

>Insert接口对POJO进行约束，要求其提供创建PreparedStatement和为PreparedStatement赋值的方法；
>
>POJO由POJOBuilder根据建表SQL语句进行构造；
>
>此处以v_tr_shop_mx为例展示POJOBuilder构造POJO类的效果；

```java
public interface Insert {
    // 构造POJO的PreparedStatement
    PreparedStatement stat(Connection con);

    // 为PreparedStatement赋值
    PreparedStatement insert(PreparedStatement preparedStatement);
}

```

```java
// 根据格式化后的SQL构造
public class POJOBuilder {
    
    public static void main(String[] args) throws IOException {
        // 读取ch.sql
        File sql = new File(ClassLoader.getSystemResource("ch.sql").getPath());
        if (sql.exists()) {
            BufferedReader in = new BufferedReader(new FileReader(sql));
            File pojo;
            String line;
            BufferedWriter out = null;
            String className = null;
            String fullName = null;
            // 维护POJO的属性和类型之间的对应关系
            Map<String, Class<?>> properties = null;
            while ((line = in.readLine()) != null) {
                line = line.trim().toLowerCase();
                // 读到建表语句，说明应当新写一个POJO类，此时记录POJO的类名，并设置BufferedWriter指向
                if (line.startsWith("create table")) {
                    line = line.substring(12).trim();
                    fullName = line;
                    className = line.substring(line.lastIndexOf('.') + 1);
                    pojo = new File(System.getProperties().getProperty("user.dir") + File.separator + "src" + File.separator + "main" + File.separator + "java" + File.separator + "pojo" + File.separator + className + ".java");
                    if ((!pojo.exists() || pojo.exists() && pojo.delete()) && pojo.createNewFile()) {
                        out = new BufferedWriter(new FileWriter(pojo));
                    }
                    properties = new HashMap<>();
                } else if (line.contains("string")) {
                    // 读到建表语句中的String，说明该属性是String类型
                    String propertiesName = line.split("\\s+")[0];
                    assert properties != null;
                    properties.put(propertiesName, String.class);
                } else if (line.contains("decimal")) {
                    // 读到建表语句中的decimal，说明该属性是Double类型
                    String propertiesName = line.split("\\s+")[0];
                    assert properties != null;
                    properties.put(propertiesName, Double.class);
                } else if (line.contains("int")) {
                    // 读到建表语句中的int，说明该属性是Integer类型
                    String propertiesName = line.split("\\s+")[0];
                    assert properties != null;
                    properties.put(propertiesName, Integer.class);
                } else if (line.startsWith(")")) {
                    // 读到），说明建表语句结束，获取POJO信息完毕，此时开始将信息写入POJO类
                    StringBuilder content = new StringBuilder();
                    // 定义换行符号和双换号符号
                    String lineSeparator = System.lineSeparator(), doubleLineSeparator = lineSeparator + lineSeparator;
                    // init，POJO的包、依赖关系及类名
                    {
                        assert out != null;
                        content.append("package pojo;").append(doubleLineSeparator);
                        content.append("import lombok.Data;").append(lineSeparator);
                        content.append("import java.sql.Connection;").append(lineSeparator);
                        content.append("import java.sql.PreparedStatement;").append(lineSeparator);
                        content.append("import java.sql.SQLException;").append(doubleLineSeparator);
                        content.append("@Data").append(lineSeparator);
                        content.append("public class ").append(className).append(" implements Insert {").append(doubleLineSeparator);
                    }
                    // properties，POJO的属性，并为double和int值赋初值（避免json空值可能造成的问题）
                    {
                        for (Map.Entry<String, Class<?>> entry : properties.entrySet()) {
                            content.append("\tpublic ").append(entry.getValue().getSimpleName()).append(" ").append(entry.getKey());
                            if (entry.getValue().equals(Integer.class)) {
                                content.append(" = 0");
                            } else if (entry.getValue().equals(Double.class)) {
                                content.append(" = 0.0");
                            }
                            content.append(";").append(doubleLineSeparator);
                        }
                    }
                    // stat & insert，实现insert接口
                    {
                        content.append("\t@Override").append(lineSeparator);
                        content.append("\tpublic PreparedStatement stat(Connection con) {").append(lineSeparator);
                        content.append("\t\ttry {").append(lineSeparator);
                        content.append("\t\t\treturn con.prepareStatement(\"INSERT INTO ").append(fullName).append(" (");
                        int index = 0;
                        // 根据属性拼接PreparedStatement需要的的sql语句
                        for (String propertiesName : properties.keySet()) {
                            content.append(propertiesName);
                            if (++index != properties.size()) content.append(", ");
                            else content.append(") VALUES (");
                        }
                        while (index-- > 1) {
                            content.append("?, ");
                        }
                        content.append("?)\");").append(lineSeparator);
                        content.append("\t\t} catch (SQLException e) {").append(lineSeparator);
                        content.append("\t\t\te.printStackTrace();").append(lineSeparator);
                        content.append("\t\t}").append(lineSeparator);
                        content.append("\t\treturn null;").append(lineSeparator);
                        content.append("\t}").append(doubleLineSeparator);

                        // 根据属性为PreparedStatement赋值
                        content.append("\t@Override").append(lineSeparator);
                        content.append("\tpublic PreparedStatement insert(PreparedStatement preparedStatement) {").append(lineSeparator);
                        content.append("\t\ttry {").append(lineSeparator);
                        for (Map.Entry<String, Class<?>> entry : properties.entrySet()) {
                            if (entry.getValue().equals(Integer.class))
                                content.append("\t\t\tpreparedStatement.setInt").append('(').append(++index).append(", ").append(entry.getKey()).append(");").append(lineSeparator);
                            else
                                content.append("\t\t\tpreparedStatement.set").append(entry.getValue().getSimpleName()).append('(').append(++index).append(", ").append(entry.getKey()).append(");").append(lineSeparator);
                        }
                        content.append("\t\t\treturn preparedStatement;").append(lineSeparator);
                        content.append("\t\t} catch (SQLException e) {").append(lineSeparator);
                        content.append("\t\t\te.printStackTrace();").append(lineSeparator);
                        content.append("\t\t}").append(lineSeparator);
                        content.append("\t\treturn preparedStatement;").append(lineSeparator);
                        content.append("\t}").append(lineSeparator);
                    }
                    // finish
                    {
                        content.append("}").append(lineSeparator);
                        out.write(content.toString());
                        out.close();
                    }
                }
            }
        }
    }
}
```

```java
package pojo;

import lombok.Data;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Data
public class v_tr_shop_mx implements Insert {

	public String tran_date;

	public String tran_channel;

	public String shop_code;

	public String hlw_tran_type;

	public String tran_time;

	public String shop_name;

	public String order_code;

	public String uid;

	public Double score_num = 0.0;

	public String current_status;

	public String pay_channel;

	public Double tran_amt = 0.0;

	public String legal_name;

	public String etl_dt;

	@Override
	public PreparedStatement stat(Connection con) {
		try {
			return con.prepareStatement("INSERT INTO dm.v_tr_shop_mx (tran_date, tran_channel, shop_code, hlw_tran_type, tran_time, shop_name, order_code, uid, score_num, current_status, pay_channel, tran_amt, legal_name, etl_dt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public PreparedStatement insert(PreparedStatement preparedStatement) {
		try {
			preparedStatement.setString(1, tran_date);
			preparedStatement.setString(2, tran_channel);
			preparedStatement.setString(3, shop_code);
			preparedStatement.setString(4, hlw_tran_type);
			preparedStatement.setString(5, tran_time);
			preparedStatement.setString(6, shop_name);
			preparedStatement.setString(7, order_code);
			preparedStatement.setString(8, uid);
			preparedStatement.setDouble(9, score_num);
			preparedStatement.setString(10, current_status);
			preparedStatement.setString(11, pay_channel);
			preparedStatement.setDouble(12, tran_amt);
			preparedStatement.setString(13, legal_name);
			preparedStatement.setString(14, etl_dt);
			return preparedStatement;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return preparedStatement;
	}
}
```



#### 2.2.4 其他

>设计了statistics表，表格仅持有两个属性（日期、交易数），为可视化提供快捷的查询方法，此处对于v_tr_shop_mx额外制造一个侧输出流并使用此StatisticSink下沉到statistics表格，使用intervals减少sql语句的执行频率。
>
>由于效率问题和存在bug而弃用。

```java
@Deprecated
public class StatisticSink extends RichSinkFunction<v_tr_shop_mx> {

    Connection connection = null;

    // 每intervals仅执行一次update
    long intervals = 2000;

    long lastModify = 0;

    PreparedStatement insert;

    PreparedStatement update;

    // 未执行的update的统计
    int count = 0;

    // 上一个shop的日期
    String lastDay;

    @Override
    public void open(Configuration parameters) {
        try {
            super.open(parameters);
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            String url = "jdbc:clickhouse://localhost:8123/dm?compress=false";
            connection = DriverManager.getConnection(url);
            connection.setAutoCommit(false);
            // 预编译sql语句
            insert = connection.prepareStatement("INSERT INTO dm.statistic_mx(tran_date, count) VALUES (?,?)");
            update = connection.prepareStatement("ALTER TABLE dm.statistic_mx UPDATE count = ((SELECT count from dm.statistic_mx where tran_date = (?)) + (?)) WHERE tran_date = (?)");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        // 如果结束时仍有update未执行，则执行之
        if (count > 0) {
            update.setInt(2, count);
            update.execute();
            connection.commit();
        }
        super.close();
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void invoke(v_tr_shop_mx shop, Context context) {
        try {
            String date = shop.tran_date;
            if (lastDay == null) {
            	// 第一次语句应为insert
                lastDay = date;
                insert.setString(1, date);
                insert.setInt(2, 1);
                insert.executeUpdate();
                update.setString(1, date);
                update.setString(3, date);

                connection.commit();

                count = 0;
                lastModify = System.currentTimeMillis();
            } else if (!lastDay.equals(date)) {
                // 新的shop类日期与之前不同，则应当组装update并执行，并且执行insert语句
                update.setString(1, lastDay);
                update.setString(3, lastDay);
                update.setInt(2, count);
                update.executeUpdate();

                insert.setString(1, date);
                insert.setInt(2, 1);
                insert.executeUpdate();

                connection.commit();

                count = 0;
                lastDay = date;
                lastModify = System.currentTimeMillis();
            } else if (System.currentTimeMillis() - lastModify > intervals) {
                // shop的日期与之前相同，且intervals内未执行过update语句，此时执行update语句并重置count
                count++;
                update.setInt(2, count);
                update.executeUpdate();
                connection.commit();

                lastModify = System.currentTimeMillis();
                count = 0;
            } else {
                // shop的日期与之前相同，且intervals内执行过update语句，此时不执行update语句，仅增加count计数
                count++;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
```

### 2.3 sql相关设计

#### 2.3.1 ch.sql 建表语句

>建表语句，使用Nullable允许空值；

```sql
create database if not exists dm;
use dm;
drop table if exists dm_v_tr_contract_mx;

CREATE TABLE dm.dm_v_tr_contract_mx
(
    uid              String,
    contract_no      Nullable(String),
    apply_no         Nullable(String),
    artificial_no    Nullable(String),
    occur_date       Nullable(String),
    loan_cust_no     Nullable(String),
    cust_name        Nullable(String),
    buss_type        Nullable(String),
    occur_type       Nullable(String),
    is_credit_cyc    Nullable(String),
    curr_type        Nullable(String),
    buss_amt         Nullable(Decimal(18, 2)),
    loan_pert        Nullable(Decimal(18, 2)),
    term_year        int,
    term_mth         int,
    term_day         int,
    base_rate_type   Nullable(String),
    base_rate        Decimal(18, 6),
    float_type       Nullable(String),
    rate_float       Decimal(18, 6),
    rate             Decimal(18, 6),
    pay_times        int,
    pay_type         Nullable(String),
    direction        Nullable(String),
    loan_use         Nullable(String),
    pay_source       Nullable(String),
    putout_date      Nullable(String),
    matu_date        Nullable(String),
    vouch_type       Nullable(String),
    is_oth_vouch     Nullable(String),
    apply_type       Nullable(String),
    extend_times     int,
    actu_out_amt     Nullable(Decimal(18, 2)),
    bal              Nullable(Decimal(18, 2)),
    norm_bal         Nullable(Decimal(18, 2)),
    dlay_bal         Nullable(Decimal(18, 2)),
    dull_bal         Nullable(Decimal(18, 2)),
    owed_int_in      Nullable(Decimal(18, 2)),
    owed_int_out     Nullable(Decimal(18, 2)),
    fine_pr_int      Nullable(Decimal(18, 2)),
    fine_intr_int    Nullable(Decimal(18, 2)),
    dlay_days        int,
    five_class       Nullable(String),
    class_date       Nullable(String),
    mge_org          Nullable(String),
    mgr_no           Nullable(String),
    operate_org      Nullable(String),
    operator         Nullable(String),
    operate_date     Nullable(String),
    reg_org          Nullable(String),
    register         Nullable(String),
    reg_date         Nullable(String),
    inte_settle_type Nullable(String),
    is_bad           Nullable(String),
    frz_amt          Decimal(18, 0),
    con_crl_type     Nullable(String),
    shift_type       Nullable(String),
    due_intr_days    int,
    reson_type       Nullable(String),
    shift_bal        Decimal(18, 0),
    is_vc_vouch      Nullable(String),
    loan_use_add     Nullable(String),
    finsh_type       Nullable(String),
    finsh_date       Nullable(String),
    sts_flag         Nullable(String),
    src_dt           Nullable(String),
    etl_dt           Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY uid;

drop table if exists dm_v_tr_djk_mx;

CREATE TABLE dm.dm_v_tr_djk_mx
(
    uid            String,
    card_no        Nullable(String),
    tran_type      Nullable(String),
    tran_type_desc Nullable(String),
    tran_amt       Nullable(Decimal(18, 2)),
    tran_amt_sign  Nullable(String),
    mer_type       Nullable(String),
    mer_code       Nullable(String),
    rev_ind        Nullable(String),
    tran_desc      Nullable(String),
    tran_date      Nullable(String),
    val_date       Nullable(String),
    pur_date       Nullable(String),
    tran_time      Nullable(String),
    acct_no        Nullable(String),
    etl_dt         Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY uid;

drop table if exists dm_v_tr_dsf_mx;

CREATE TABLE dm.dm_v_tr_dsf_mx
(
    tran_date       String,
    tran_log_no     Nullable(String),
    tran_code       Nullable(String),
    channel_flg     Nullable(String),
    tran_org        Nullable(String),
    tran_teller_no  Nullable(String),
    dc_flag         Nullable(String),
    tran_amt        Nullable(Decimal(18, 2)),
    send_bank       Nullable(String),
    payer_open_bank Nullable(String),
    payer_acct_no   Nullable(String),
    payer_name      Nullable(String),
    payee_open_bank Nullable(String),
    payee_acct_no   Nullable(String),
    payee_name      Nullable(String),
    tran_sts        Nullable(String),
    busi_type       Nullable(String),
    busi_sub_type   Nullable(String),
    etl_dt          Nullable(String),
    uid             Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY tran_date;

drop table if exists dm_v_tr_duebill_mx;

CREATE TABLE dm.dm_v_tr_duebill_mx
(
    uid            String,
    acct_no        Nullable(String),
    receipt_no     Nullable(String),
    contract_no    Nullable(String),
    subject_no     Nullable(String),
    cust_no        Nullable(String),
    loan_cust_no   Nullable(String),
    cust_name      Nullable(String),
    buss_type      Nullable(String),
    curr_type      Nullable(String),
    buss_amt       Nullable(Decimal(18, 2)),
    putout_date    Nullable(String),
    matu_date      Nullable(String),
    actu_matu_date Nullable(String),
    buss_rate      Decimal(31, 10),
    actu_buss_rate Decimal(31, 10),
    intr_type      Nullable(String),
    intr_cyc       Nullable(String),
    pay_times      int,
    pay_cyc        Nullable(String),
    extend_times   int,
    bal            Nullable(Decimal(18, 2)),
    norm_bal       Nullable(Decimal(18, 2)),
    dlay_amt       Nullable(Decimal(18, 2)),
    dull_amt       Decimal(31, 10),
    bad_debt_amt   Decimal(31, 10),
    owed_int_in    Nullable(Decimal(18, 2)),
    owed_int_out   Nullable(Decimal(18, 2)),
    fine_pr_int    Nullable(Decimal(18, 2)),
    fine_intr_int  Nullable(Decimal(18, 2)),
    dlay_days      int,
    pay_acct       Nullable(String),
    putout_acct    Nullable(String),
    pay_back_acct  Nullable(String),
    due_intr_days  int,
    operate_org    Nullable(String),
    operator       Nullable(String),
    reg_org        Nullable(String),
    register       Nullable(String),
    occur_date     Nullable(String),
    loan_use       Nullable(String),
    pay_type       Nullable(String),
    pay_freq       Nullable(String),
    vouch_type     Nullable(String),
    mgr_no         Nullable(String),
    mge_org        Nullable(String),
    loan_channel   Nullable(String),
    ten_class      Nullable(String),
    src_dt         Nullable(String),
    etl_dt         Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY uid;

drop table if exists dm_v_tr_etc_mx;

CREATE TABLE dm.dm_v_tr_etc_mx
(
    uid          String,
    etc_acct     Nullable(String),
    card_no      Nullable(String),
    car_no       Nullable(String),
    cust_name    Nullable(String),
    tran_date    Nullable(String),
    tran_time    Nullable(String),
    tran_amt_fen Nullable(Decimal(18, 2)),
    real_amt     Nullable(Decimal(18, 2)),
    conces_amt   Nullable(Decimal(18, 2)),
    tran_place   Nullable(String),
    mob_phone    Nullable(String),
    etl_dt       Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY uid;

drop table if exists dm_v_tr_grwy_mx;

CREATE TABLE dm.dm_v_tr_grwy_mx
(
    uid             String,
    mch_channel     Nullable(String),
    login_type      Nullable(String),
    ebank_cust_no   Nullable(String),
    tran_date       Nullable(String),
    tran_time       Nullable(String),
    tran_code       Nullable(String),
    tran_sts        Nullable(String),
    return_code     Nullable(String),
    return_msg      Nullable(String),
    sys_type        Nullable(String),
    payer_acct_no   Nullable(String),
    payer_acct_name Nullable(String),
    payee_acct_no   Nullable(String),
    payee_acct_name Nullable(String),
    tran_amt        Nullable(Decimal(18, 2)),
    etl_dt          Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY uid;

drop table if exists dm_v_tr_gzdf_mx;

CREATE TABLE dm.dm_v_tr_gzdf_mx
(
    belong_org   String,
    ent_acct     Nullable(String),
    ent_name     Nullable(String),
    eng_cert_no  Nullable(String),
    acct_no      Nullable(String),
    cust_name    Nullable(String),
    uid          Nullable(String),
    tran_date    Nullable(String),
    tran_amt     Nullable(Decimal(18, 2)),
    tran_log_no  Nullable(String),
    is_secu_card Nullable(String),
    trna_channel Nullable(String),
    batch_no     Nullable(String),
    etl_dt       Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY belong_org;

drop table if exists dm_v_tr_huanb_mx;

CREATE TABLE dm.dm_v_tr_huanb_mx
(
    tran_flag       String,
    uid             Nullable(String),
    cust_name       Nullable(String),
    acct_no         Nullable(String),
    tran_date       Nullable(String),
    tran_time       Nullable(String),
    tran_amt        Nullable(Decimal(18, 2)),
    bal             Nullable(Decimal(18, 2)),
    tran_code       Nullable(String),
    dr_cr_code      Nullable(String),
    pay_term        int,
    tran_teller_no  Nullable(String),
    pprd_rfn_amt    Nullable(Decimal(18, 2)),
    pprd_amotz_intr Nullable(Decimal(18, 2)),
    tran_log_no     Nullable(String),
    tran_type       Nullable(String),
    dscrp_code      Nullable(String),
    remark          Nullable(String),
    etl_dt          Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY tran_flag;

drop table if exists dm_v_tr_huanx_mx;

CREATE TABLE dm.dm_v_tr_huanx_mx
(
    tran_flag      String,
    uid            Nullable(String),
    cust_name      Nullable(String),
    acct_no        Nullable(String),
    tran_date      Nullable(String),
    tran_time      Nullable(String),
    tran_amt       Nullable(Decimal(18, 2)),
    cac_intc_pr    Nullable(Decimal(18, 2)),
    tran_code      Nullable(String),
    dr_cr_code     Nullable(String),
    pay_term       int,
    tran_teller_no Nullable(String),
    intc_strt_date Nullable(String),
    intc_end_date  Nullable(String),
    intr           Nullable(Decimal(18, 2)),
    tran_log_no    Nullable(String),
    tran_type      Nullable(String),
    dscrp_code     Nullable(String),
    etl_dt         Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY tran_flag;

drop table if exists dm_v_tr_sa_mx;

CREATE TABLE dm.dm_v_tr_sa_mx
(
    uid            String,
    card_no        Nullable(String),
    cust_name      Nullable(String),
    acct_no        Nullable(String),
    det_n          int,
    curr_type      Nullable(String),
    tran_teller_no Nullable(String),
    cr_amt         Nullable(Decimal(18, 2)),
    bal            Nullable(Decimal(18, 2)),
    tran_amt       Nullable(Decimal(18, 2)),
    tran_card_no   Nullable(String),
    tran_type      Nullable(String),
    tran_log_no    Nullable(String),
    dr_amt         Nullable(Decimal(18, 2)),
    open_org       Nullable(String),
    dscrp_code     Nullable(String),
    remark         Nullable(String),
    tran_time      Nullable(String),
    tran_date      String,
    sys_date       Nullable(String),
    tran_code      Nullable(String),
    remark_1       Nullable(String),
    oppo_cust_name Nullable(String),
    agt_cert_type  Nullable(String),
    agt_cert_no    Nullable(String),
    agt_cust_name  Nullable(String),
    channel_flag   Nullable(String),
    oppo_acct_no   Nullable(String),
    oppo_bank_no   Nullable(String),
    src_dt         Nullable(String),
    etl_dt         Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY uid
        PARTITION BY tran_date;

drop table if exists dm_v_tr_sbyb_mx;

CREATE TABLE dm.dm_v_tr_sbyb_mx
(
    uid            String,
    cust_name      Nullable(String),
    tran_date      Nullable(String),
    tran_sts       Nullable(String),
    tran_org       Nullable(String),
    tran_teller_no Nullable(String),
    tran_amt_fen   Nullable(Decimal(18, 2)),
    tran_type      Nullable(String),
    return_msg     Nullable(String),
    etl_dt         Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY uid;

drop table if exists dm_v_tr_sdrq_mx;

CREATE TABLE dm.dm_v_tr_sdrq_mx
(
    hosehld_no     String,
    acct_no        Nullable(String),
    cust_name      Nullable(String),
    tran_type      Nullable(String),
    tran_date      Nullable(String),
    tran_amt_fen   Nullable(Decimal(18, 2)),
    channel_flg    Nullable(String),
    tran_org       Nullable(String),
    tran_teller_no Nullable(String),
    tran_log_no    Nullable(String),
    batch_no       Nullable(String),
    tran_sts       Nullable(String),
    return_msg     Nullable(String),
    etl_dt         Nullable(String),
    uid            Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY hosehld_no;

drop table if exists dm_v_tr_sjyh_mx;

CREATE TABLE dm.dm_v_tr_sjyh_mx
(
    uid             String,
    mch_channel     Nullable(String),
    login_type      Nullable(String),
    ebank_cust_no   Nullable(String),
    tran_date       Nullable(String),
    tran_time       Nullable(String),
    tran_code       Nullable(String),
    tran_sts        Nullable(String),
    return_code     Nullable(String),
    return_msg      Nullable(String),
    sys_type        Nullable(String),
    payer_acct_no   Nullable(String),
    payer_acct_name Nullable(String),
    payee_acct_no   Nullable(String),
    payee_acct_name Nullable(String),
    tran_amt        Nullable(Decimal(18, 2)),
    etl_dt          Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY uid;

drop table if exists v_tr_shop_mx;

CREATE TABLE dm.v_tr_shop_mx
(
    tran_channel   String,
    order_code     Nullable(String),
    shop_code      Nullable(String),
    shop_name      Nullable(String),
    hlw_tran_type  Nullable(String),
    tran_date      Nullable(String),
    tran_time      Nullable(String),
    tran_amt       Nullable(Decimal(18, 2)),
    current_status Nullable(String),
    score_num      Nullable(Decimal(18, 2)),
    pay_channel    Nullable(String),
    uid            Nullable(String),
    legal_name     Nullable(String),
    etl_dt         Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY tran_channel;

drop table if exists statistic_mx;

CREATE TABLE dm.statistic_mx
(
    tran_date      String,
    count          int
)
    ENGINE = MergeTree
        ORDER BY tran_date;

```

#### 2.3.2 select.sql 工具语句

>查询所有表格的数据行数

```sql
use dm;
select (select count(1) from v_tr_shop_mx)        as shop,
       (select count(1) from dm_v_tr_sa_mx)       as sa,
       (select count(1) from dm_v_tr_sjyh_mx)     as sjyh,
       (select count(1) from dm_v_tr_sdrq_mx)     as sdrq,
       (select count(1) from dm_v_tr_sbyb_mx)     as sbyb,
       (select count(1) from dm_v_tr_huanx_mx)    as huanx,
       (select count(1) from dm_v_tr_huanb_mx)    as huanb,
       (select count(1) from dm_v_tr_etc_mx)      as etc,
       (select count(1) from dm_v_tr_gzdf_mx)     as gzdf,
       (select count(1) from dm_v_tr_duebill_mx)  as duebill,
       (select count(1) from dm_v_tr_grwy_mx)     as grwy,
       (select count(1) from dm_v_tr_dsf_mx)      as dsf,
       (select count(1) from dm_v_tr_djk_mx)      as djk,
       (select count(1) from dm_v_tr_contract_mx) as contract,
       shop + sa + sjyh + sdrq + sbyb + huanx + huanb + etc + gzdf + duebill + grwy + dsf + djk + contract
                                                  as count;
```

### 2.4 可视化设计

#### 2.4.1 后端设计

>基础的查询每日的shop订单数目，效率满足要求；

```java
@RequestMapping("/select")
@RestController
public class selectController {

    Connection connection = null;

    PreparedStatement preparedStatement;

    @PostConstruct
    void init() {
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            String url = "jdbc:clickhouse://124.222.139.8:8123/dm?compress=false";
            // String url = "jdbc:clickhouse://localhost:8123/dm?compress=false";
            connection = DriverManager.getConnection(url);
            preparedStatement = connection.prepareStatement("SELECT count(1), tran_date FROM dm.v_tr_shop_mx group by tran_date order by tran_date");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @GetMapping("/all")
    @CrossOrigin
    public List<String[]> select() {
        try {
            List<String[]> res = new ArrayList<>();
            ResultSet set = preparedStatement.executeQuery();
            while (set.next()) {
                res.add(new String[]{set.getString(2), set.getString(1)});
            }
            return res;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
```

#### 2.4.2 前端设计

>使用echart绘制图标，每秒调用get()方法，向后端请求最新数据更新图标，实现动态可视化每日的订单数目变化

```html
<!DOCTYPE html>
<html>
<head>
  <title>Echarts 3画图</title>
  <meta charset="utf-8" />
  <script src="https://cdn.staticfile.org/echarts/4.3.0/echarts.js"></script>
  <script src="https://cdn.staticfile.org/jquery/1.10.2/jquery.min.js"></script>
</head>
<body>
<div id="chart" style="width: 1280px; height: 720px">
</div>
<!-- 下面是使用ECharts绘制图表的代码 -->
<script>


    // 模拟数据
  let allData=[[['2021-4-1','12314'],['2021-4-2',44314]],[['2021-4-2',64324],['2021-4-3',54324],['2021-4-4',385493],['2021-4-5',496837]],[['2021-4-6',958463],['2021-4-7',938475],['2021-4-8',146837]],[['2021-4-9',927464],
    ['2021-4-10',414124],['2021-4-11',736594],['2021-4-12',876253]],[['2021-4-13',918274],['2021-4-14',638273],['2021-4-15',563827],['2021-4-16',726354]]]

  let nowData=[]
  let nowDate=[]
  let now=0;

  // 创建ECharts对象
  var myChart = echarts.init(document.getElementById("chart"))
  let startTime=new Date(2021,3,1).getTime()

  function get(){
    $.ajax({url:"http://localhost:8080/select/all",success:function(result){
        update(result)
      }});
  }

  function update(result) {
    nowData=[];nowDate=[];
    for (let i=0;i<result.length;i++) {
      nowData[i]=result[i][1]
      nowDate[i]=result[i][0]
    }
    // 配置图表
    var option = {
      // 配置X轴
      xAxis: {
        type: "category",
        data: nowDate
      },
      // 配置Y轴
      yAxis: {
        scale: true
      },
      series: [{
        name: '销量',
        type: "line",
        data: nowData
      }],
      dataZoom: [
        {
          type: "inside"
        },
        {
          type: "slider",
          handleIcon: "M10.7,11.9v-1.3H9.3v1.3c-4.9,0.3-8.8,4.4-8.8,9.4c0,5,3.9,9.1,8.8,9.4v1.3h1.3v-1.3c4.9-0.3,8.8-4.4,8.8-9.4C19.5,16.3,15.6,12.2,10.7,11.9z M13.3,24.4H6.7V23h6.6V24.4z M13.3,19.6H6.7v-1.4h6.6V19.6z",
          handleStyle: {
            color: "#55AFE1"
          },
          dataBackground: {
            areaStyle: {
              color: "#90D4EB"
            },
            lineStyle: {
              opacity: 1.0,
              color: "#52A1CA"
            }
          }
        }
      ],
      tooltip: {
        trigger: "item",
      }
    }
    // 启用配置
    myChart.setOption(option)
  }
  
   setInterval(  //设置定时器，1s更新一次
       function(){
           get();
       },1000
   );


</script>
</body>
</html>
```

## 3. 效果展示

### 3.1 fink相关

>在满足可视化要求的情况下，累计执行**1h 59m 58s**完成job（此处未贴图）
>
>flink使用jar包见文件”assets/kafka-1.0-SNAPSHOT-jar-with-dependencies.jar”

![2022-04-27 13-20-26 的屏幕截图](https://raw.githubusercontent.com/Leonardo-hf/DI-Homework-stage-2/master/assets/2022-04-27 13-20-26 的屏幕截图.png)

![2022-04-27 13-20-41 的屏幕截图](https://raw.githubusercontent.com/Leonardo-hf/DI-Homework-stage-2/master/assets/2022-04-27 13-20-41 的屏幕截图.png)

### 3.2 kafka相关

>此处贴出生产者生产数据时log线程的输出，可见生产者共生产25802860条数据，与clickhouse获得的总数据相一致；

![2022-04-27 13-57-34 的屏幕截图](https://raw.githubusercontent.com/Leonardo-hf/DI-Homework-stage-2/master/assets/2022-04-27 13-57-34 的屏幕截图.png)

### 3.3 clickhouse相关

>select.sql执行结果，共收集到**25800205**条数据，此处以dm_v_tr_grwy_mx表格为例（数据量少）展示数据收集情况

![2022-04-27 13-31-43 的屏幕截图](https://raw.githubusercontent.com/Leonardo-hf/DI-Homework-stage-2/master/assets/2022-04-27 13-31-43 的屏幕截图.png)

![2022-04-27 13-33-39 的屏幕截图](https://raw.githubusercontent.com/Leonardo-hf/DI-Homework-stage-2/master/assets/2022-04-27 13-33-39 的屏幕截图.png)

### 3.4 可视化相关

>下图为完整数据的可视化展示，6.14日后没有接收到数据直到11.24日后有限几日；
>
>该图标可以使用滚轮或拖动图标下方进度轴，放大某阶段具体数据，如图二；
>
>图表的动态变化见视频“assets/view.mp4”，6.14到11.24日之间隔了几分钟，就没录制了；

![2022-04-27 13-37-37 的屏幕截图](https://raw.githubusercontent.com/Leonardo-hf/DI-Homework-stage-2/master/assets/2022-04-27 13-37-37 的屏幕截图.png)

![2022-04-27 13-37-51 的屏幕截图](https://raw.githubusercontent.com/Leonardo-hf/DI-Homework-stage-2/master/assets/2022-04-27 13-37-51 的屏幕截图.png)

<video id="video" controls=""src="https://raw.githubusercontent.com/Leonardo-hf/DI-Homework-stage-2/master/assets/view.mp4" preload="none">

