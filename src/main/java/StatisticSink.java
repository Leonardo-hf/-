import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import pojo.v_tr_shop_mx;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

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
