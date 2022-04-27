package pojo;

import lombok.Data;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Data
public class dm_v_tr_etc_mx implements Insert {

	public String tran_date;

	public Double real_amt = 0.0;

	public Double tran_amt_fen = 0.0;

	public String tran_place;

	public String tran_time;

	public Double conces_amt = 0.0;

	public String mob_phone;

	public String uid;

	public String card_no;

	public String car_no;

	public String cust_name;

	public String etl_dt;

	public String etc_acct;

	@Override
	public PreparedStatement stat(Connection con) {
		try {
			return con.prepareStatement("INSERT INTO dm.dm_v_tr_etc_mx (tran_date, real_amt, tran_amt_fen, tran_place, tran_time, conces_amt, mob_phone, uid, card_no, car_no, cust_name, etl_dt, etc_acct) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public PreparedStatement insert(PreparedStatement preparedStatement) {
		try {
			preparedStatement.setString(1, tran_date);
			preparedStatement.setDouble(2, real_amt);
			preparedStatement.setDouble(3, tran_amt_fen);
			preparedStatement.setString(4, tran_place);
			preparedStatement.setString(5, tran_time);
			preparedStatement.setDouble(6, conces_amt);
			preparedStatement.setString(7, mob_phone);
			preparedStatement.setString(8, uid);
			preparedStatement.setString(9, card_no);
			preparedStatement.setString(10, car_no);
			preparedStatement.setString(11, cust_name);
			preparedStatement.setString(12, etl_dt);
			preparedStatement.setString(13, etc_acct);
			return preparedStatement;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return preparedStatement;
	}
}
