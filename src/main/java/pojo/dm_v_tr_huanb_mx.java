package pojo;

import lombok.Data;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Data
public class dm_v_tr_huanb_mx implements Insert {

	public String tran_date;

	public String tran_code;

	public Double pprd_rfn_amt = 0.0;

	public String tran_flag;

	public String tran_type;

	public Integer pay_term = 0;

	public String dscrp_code;

	public String remark;

	public String tran_time;

	public Double bal = 0.0;

	public Double pprd_amotz_intr = 0.0;

	public String uid;

	public String acct_no;

	public String dr_cr_code;

	public String tran_teller_no;

	public String cust_name;

	public Double tran_amt = 0.0;

	public String tran_log_no;

	public String etl_dt;

	@Override
	public PreparedStatement stat(Connection con) {
		try {
			return con.prepareStatement("INSERT INTO dm.dm_v_tr_huanb_mx (tran_date, tran_code, pprd_rfn_amt, tran_flag, tran_type, pay_term, dscrp_code, remark, tran_time, bal, pprd_amotz_intr, uid, acct_no, dr_cr_code, tran_teller_no, cust_name, tran_amt, tran_log_no, etl_dt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public PreparedStatement insert(PreparedStatement preparedStatement) {
		try {
			preparedStatement.setString(1, tran_date);
			preparedStatement.setString(2, tran_code);
			preparedStatement.setDouble(3, pprd_rfn_amt);
			preparedStatement.setString(4, tran_flag);
			preparedStatement.setString(5, tran_type);
			preparedStatement.setInt(6, pay_term);
			preparedStatement.setString(7, dscrp_code);
			preparedStatement.setString(8, remark);
			preparedStatement.setString(9, tran_time);
			preparedStatement.setDouble(10, bal);
			preparedStatement.setDouble(11, pprd_amotz_intr);
			preparedStatement.setString(12, uid);
			preparedStatement.setString(13, acct_no);
			preparedStatement.setString(14, dr_cr_code);
			preparedStatement.setString(15, tran_teller_no);
			preparedStatement.setString(16, cust_name);
			preparedStatement.setDouble(17, tran_amt);
			preparedStatement.setString(18, tran_log_no);
			preparedStatement.setString(19, etl_dt);
			return preparedStatement;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return preparedStatement;
	}
}
