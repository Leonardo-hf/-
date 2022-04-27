import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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