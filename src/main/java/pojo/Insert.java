package pojo;

import java.sql.Connection;
import java.sql.PreparedStatement;

public interface Insert {
    // 构造POJO的PreparedStatement
    PreparedStatement stat(Connection con);

    // 为PreparedStatement赋值
    PreparedStatement insert(PreparedStatement preparedStatement);
}
