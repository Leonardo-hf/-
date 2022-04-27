package pojo;

import lombok.Data;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Data
public class dm_v_tr_sbyb_mx implements Insert {

	public String uid;

	public String tran_date;

	public String tran_sts;

	public String tran_type;

	public String tran_teller_no;

	public Double tran_amt_fen = 0.0;

	public String cust_name;

	public String return_msg;

	public String etl_dt;

	public String tran_org;

	@Override
	public PreparedStatement stat(Connection con) {
		try {
			return con.prepareStatement("INSERT INTO dm.dm_v_tr_sbyb_mx (uid, tran_date, tran_sts, tran_type, tran_teller_no, tran_amt_fen, cust_name, return_msg, etl_dt, tran_org) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public PreparedStatement insert(PreparedStatement preparedStatement) {
		try {
			preparedStatement.setString(1, uid);
			preparedStatement.setString(2, tran_date);
			preparedStatement.setString(3, tran_sts);
			preparedStatement.setString(4, tran_type);
			preparedStatement.setString(5, tran_teller_no);
			preparedStatement.setDouble(6, tran_amt_fen);
			preparedStatement.setString(7, cust_name);
			preparedStatement.setString(8, return_msg);
			preparedStatement.setString(9, etl_dt);
			preparedStatement.setString(10, tran_org);
			return preparedStatement;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return preparedStatement;
	}
}
