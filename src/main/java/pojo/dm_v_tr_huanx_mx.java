package pojo;

import lombok.Data;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Data
public class dm_v_tr_huanx_mx implements Insert {

	public String tran_date;

	public String tran_code;

	public String intc_strt_date;

	public String tran_flag;

	public String tran_type;

	public Integer pay_term = 0;

	public String dscrp_code;

	public String tran_time;

	public String intc_end_date;

	public String uid;

	public Double intr = 0.0;

	public Double cac_intc_pr = 0.0;

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
			return con.prepareStatement("INSERT INTO dm.dm_v_tr_huanx_mx (tran_date, tran_code, intc_strt_date, tran_flag, tran_type, pay_term, dscrp_code, tran_time, intc_end_date, uid, intr, cac_intc_pr, acct_no, dr_cr_code, tran_teller_no, cust_name, tran_amt, tran_log_no, etl_dt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
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
			preparedStatement.setString(3, intc_strt_date);
			preparedStatement.setString(4, tran_flag);
			preparedStatement.setString(5, tran_type);
			preparedStatement.setInt(6, pay_term);
			preparedStatement.setString(7, dscrp_code);
			preparedStatement.setString(8, tran_time);
			preparedStatement.setString(9, intc_end_date);
			preparedStatement.setString(10, uid);
			preparedStatement.setDouble(11, intr);
			preparedStatement.setDouble(12, cac_intc_pr);
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
