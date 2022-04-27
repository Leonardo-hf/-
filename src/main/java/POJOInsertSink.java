import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import pojo.Insert;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class POJOInsertSink extends RichSinkFunction<Insert> {

    // 数据库连接
    Connection connection = null;

    // 批量插入的下限，不应超过生产数据的批量（16384），
    // 设置较大时，插入效率更高，但插入频率降低不利于可视化，在可视化展示时可合理降低standard值，建议为512
    final int standard = 64;

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
             String url = "jdbc:clickhouse://localhost:8123/dm?compress=false";
//            String url = "jdbc:clickhouse://124.222.139.8:8123/dm?compress=false";
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