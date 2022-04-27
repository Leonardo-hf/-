package pojo;

import lombok.Data;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Data
public class dm_v_tr_sdrq_mx implements Insert {

	public String tran_date;

	public String batch_no;

	public String tran_type;

	public Double tran_amt_fen = 0.0;

	public String return_msg;

	public String hosehld_no;

	public String tran_sts;

	public String uid;

	public String acct_no;

	public String tran_teller_no;

	public String cust_name;

	public String tran_log_no;

	public String etl_dt;

	public String channel_flg;

	public String tran_org;

	@Override
	public PreparedStatement stat(Connection con) {
		try {
			return con.prepareStatement("INSERT INTO dm.dm_v_tr_sdrq_mx (tran_date, batch_no, tran_type, tran_amt_fen, return_msg, hosehld_no, tran_sts, uid, acct_no, tran_teller_no, cust_name, tran_log_no, etl_dt, channel_flg, tran_org) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public PreparedStatement insert(PreparedStatement preparedStatement) {
		try {
			preparedStatement.setString(1, tran_date);
			preparedStatement.setString(2, batch_no);
			preparedStatement.setString(3, tran_type);
			preparedStatement.setDouble(4, tran_amt_fen);
			preparedStatement.setString(5, return_msg);
			preparedStatement.setString(6, hosehld_no);
			preparedStatement.setString(7, tran_sts);
			preparedStatement.setString(8, uid);
			preparedStatement.setString(9, acct_no);
			preparedStatement.setString(10, tran_teller_no);
			preparedStatement.setString(11, cust_name);
			preparedStatement.setString(12, tran_log_no);
			preparedStatement.setString(13, etl_dt);
			preparedStatement.setString(14, channel_flg);
			preparedStatement.setString(15, tran_org);
			return preparedStatement;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return preparedStatement;
	}
}
