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
