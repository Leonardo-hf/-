import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import pojo.*;

import java.util.Properties;

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
        String[] keys = new String[]{"shop"/*, "sjyh", "sdrq", "sbyb", "sa", "huanx", "huanb", "gzdf", "grwy", "etc", "duebill", "dsf", "djk", "contract"*/};
        Class<Insert>[] classes = new Class[]{v_tr_shop_mx.class/*, dm_v_tr_sjyh_mx.class, dm_v_tr_sdrq_mx.class, dm_v_tr_sbyb_mx.class, dm_v_tr_sa_mx.class, dm_v_tr_huanx_mx.class, dm_v_tr_huanb_mx.class, dm_v_tr_gzdf_mx.class, dm_v_tr_grwy_mx.class, dm_v_tr_etc_mx.class, dm_v_tr_duebill_mx.class, dm_v_tr_dsf_mx.class, dm_v_tr_djk_mx.class, dm_v_tr_contract_mx.class*/};
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