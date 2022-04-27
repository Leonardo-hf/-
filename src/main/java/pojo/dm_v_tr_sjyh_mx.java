package pojo;

import lombok.Data;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Data
public class dm_v_tr_sjyh_mx implements Insert {

	public String tran_date;

	public String ebank_cust_no;

	public String tran_code;

	public String login_type;

	public String return_msg;

	public String tran_time;

	public String sys_type;

	public String payee_acct_no;

	public String payer_acct_no;

	public String uid;

	public String tran_sts;

	public String payee_acct_name;

	public String mch_channel;

	public Double tran_amt = 0.0;

	public String etl_dt;

	public String return_code;

	public String payer_acct_name;

	@Override
	public PreparedStatement stat(Connection con) {
		try {
			return con.prepareStatement("INSERT INTO dm.dm_v_tr_sjyh_mx (tran_date, ebank_cust_no, tran_code, login_type, return_msg, tran_time, sys_type, payee_acct_no, payer_acct_no, uid, tran_sts, payee_acct_name, mch_channel, tran_amt, etl_dt, return_code, payer_acct_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public PreparedStatement insert(PreparedStatement preparedStatement) {
		try {
			preparedStatement.setString(1, tran_date);
			preparedStatement.setString(2, ebank_cust_no);
			preparedStatement.setString(3, tran_code);
			preparedStatement.setString(4, login_type);
			preparedStatement.setString(5, return_msg);
			preparedStatement.setString(6, tran_time);
			preparedStatement.setString(7, sys_type);
			preparedStatement.setString(8, payee_acct_no);
			preparedStatement.setString(9, payer_acct_no);
			preparedStatement.setString(10, uid);
			preparedStatement.setString(11, tran_sts);
			preparedStatement.setString(12, payee_acct_name);
			preparedStatement.setString(13, mch_channel);
			preparedStatement.setDouble(14, tran_amt);
			preparedStatement.setString(15, etl_dt);
			preparedStatement.setString(16, return_code);
			preparedStatement.setString(17, payer_acct_name);
			return preparedStatement;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return preparedStatement;
	}
}
