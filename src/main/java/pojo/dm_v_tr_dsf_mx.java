package pojo;

import lombok.Data;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Data
public class dm_v_tr_dsf_mx implements Insert {

	public String payer_open_bank;

	public String tran_date;

	public String tran_code;

	public String dc_flag;

	public String busi_sub_type;

	public String payee_acct_no;

	public String payer_acct_no;

	public String payee_open_bank;

	public String tran_sts;

	public String uid;

	public String busi_type;

	public String payee_name;

	public String tran_teller_no;

	public Double tran_amt = 0.0;

	public String tran_log_no;

	public String send_bank;

	public String payer_name;

	public String etl_dt;

	public String channel_flg;

	public String tran_org;

	@Override
	public PreparedStatement stat(Connection con) {
		try {
			return con.prepareStatement("INSERT INTO dm.dm_v_tr_dsf_mx (payer_open_bank, tran_date, tran_code, dc_flag, busi_sub_type, payee_acct_no, payer_acct_no, payee_open_bank, tran_sts, uid, busi_type, payee_name, tran_teller_no, tran_amt, tran_log_no, send_bank, payer_name, etl_dt, channel_flg, tran_org) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public PreparedStatement insert(PreparedStatement preparedStatement) {
		try {
			preparedStatement.setString(1, payer_open_bank);
			preparedStatement.setString(2, tran_date);
			preparedStatement.setString(3, tran_code);
			preparedStatement.setString(4, dc_flag);
			preparedStatement.setString(5, busi_sub_type);
			preparedStatement.setString(6, payee_acct_no);
			preparedStatement.setString(7, payer_acct_no);
			preparedStatement.setString(8, payee_open_bank);
			preparedStatement.setString(9, tran_sts);
			preparedStatement.setString(10, uid);
			preparedStatement.setString(11, busi_type);
			preparedStatement.setString(12, payee_name);
			preparedStatement.setString(13, tran_teller_no);
			preparedStatement.setDouble(14, tran_amt);
			preparedStatement.setString(15, tran_log_no);
			preparedStatement.setString(16, send_bank);
			preparedStatement.setString(17, payer_name);
			preparedStatement.setString(18, etl_dt);
			preparedStatement.setString(19, channel_flg);
			preparedStatement.setString(20, tran_org);
			return preparedStatement;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return preparedStatement;
	}
}
