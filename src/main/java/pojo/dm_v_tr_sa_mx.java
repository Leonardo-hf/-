package pojo;

import lombok.Data;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Data
public class dm_v_tr_sa_mx implements Insert {

	public Double dr_amt = 0.0;

	public String sys_date;

	public String curr_type;

	public String oppo_acct_no;

	public String dscrp_code;

	public String remark;

	public String tran_time;

	public String channel_flag;

	public Double bal = 0.0;

	public String uid;

	public String card_no;

	public String remark_1;

	public String oppo_cust_name;

	public String tran_teller_no;

	public String cust_name;

	public Double cr_amt = 0.0;

	public String tran_card_no;

	public String etl_dt;

	public String oppo_bank_no;

	public String open_org;

	public String tran_date;

	public String tran_code;

	public String tran_type;

	public String agt_cust_name;

	public String agt_cert_no;

	public Integer det_n = 0;

	public String acct_no;

	public String src_dt;

	public Double tran_amt = 0.0;

	public String tran_log_no;

	public String agt_cert_type;

	@Override
	public PreparedStatement stat(Connection con) {
		try {
			return con.prepareStatement("INSERT INTO dm.dm_v_tr_sa_mx (dr_amt, sys_date, curr_type, oppo_acct_no, dscrp_code, remark, tran_time, channel_flag, bal, uid, card_no, remark_1, oppo_cust_name, tran_teller_no, cust_name, cr_amt, tran_card_no, etl_dt, oppo_bank_no, open_org, tran_date, tran_code, tran_type, agt_cust_name, agt_cert_no, det_n, acct_no, src_dt, tran_amt, tran_log_no, agt_cert_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public PreparedStatement insert(PreparedStatement preparedStatement) {
		try {
			preparedStatement.setDouble(1, dr_amt);
			preparedStatement.setString(2, sys_date);
			preparedStatement.setString(3, curr_type);
			preparedStatement.setString(4, oppo_acct_no);
			preparedStatement.setString(5, dscrp_code);
			preparedStatement.setString(6, remark);
			preparedStatement.setString(7, tran_time);
			preparedStatement.setString(8, channel_flag);
			preparedStatement.setDouble(9, bal);
			preparedStatement.setString(10, uid);
			preparedStatement.setString(11, card_no);
			preparedStatement.setString(12, remark_1);
			preparedStatement.setString(13, oppo_cust_name);
			preparedStatement.setString(14, tran_teller_no);
			preparedStatement.setString(15, cust_name);
			preparedStatement.setDouble(16, cr_amt);
			preparedStatement.setString(17, tran_card_no);
			preparedStatement.setString(18, etl_dt);
			preparedStatement.setString(19, oppo_bank_no);
			preparedStatement.setString(20, open_org);
			preparedStatement.setString(21, tran_date);
			preparedStatement.setString(22, tran_code);
			preparedStatement.setString(23, tran_type);
			preparedStatement.setString(24, agt_cust_name);
			preparedStatement.setString(25, agt_cert_no);
			preparedStatement.setInt(26, det_n);
			preparedStatement.setString(27, acct_no);
			preparedStatement.setString(28, src_dt);
			preparedStatement.setDouble(29, tran_amt);
			preparedStatement.setString(30, tran_log_no);
			preparedStatement.setString(31, agt_cert_type);
			return preparedStatement;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return preparedStatement;
	}
}
