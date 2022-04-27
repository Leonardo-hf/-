package pojo;

import lombok.Data;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Data
public class dm_v_tr_duebill_mx implements Insert {

	public String reg_org;

	public String receipt_no;

	public String pay_cyc;

	public String intr_type;

	public String pay_freq;

	public Double owed_int_out = 0.0;

	public Double bal = 0.0;

	public Double owed_int_in = 0.0;

	public String operator;

	public Integer extend_times = 0;

	public String cust_no;

	public Double buss_rate = 0.0;

	public String vouch_type;

	public Double fine_intr_int = 0.0;

	public String pay_back_acct;

	public String etl_dt;

	public Double actu_buss_rate = 0.0;

	public String putout_acct;

	public Double fine_pr_int = 0.0;

	public String mgr_no;

	public Integer pay_times = 0;

	public String loan_use;

	public Integer dlay_days = 0;

	public String actu_matu_date;

	public String occur_date;

	public String buss_type;

	public String mge_org;

	public String acct_no;

	public Double dlay_amt = 0.0;

	public String matu_date;

	public Double norm_bal = 0.0;

	public Double dull_amt = 0.0;

	public Double bad_debt_amt = 0.0;

	public String loan_channel;

	public String curr_type;

	public String uid;

	public String operate_org;

	public Double buss_amt = 0.0;

	public String putout_date;

	public String cust_name;

	public Integer due_intr_days = 0;

	public String pay_type;

	public String ten_class;

	public String contract_no;

	public String pay_acct;

	public String src_dt;

	public String subject_no;

	public String loan_cust_no;

	public String intr_cyc;

	public String register;

	@Override
	public PreparedStatement stat(Connection con) {
		try {
			return con.prepareStatement("INSERT INTO dm.dm_v_tr_duebill_mx (reg_org, receipt_no, pay_cyc, intr_type, pay_freq, owed_int_out, bal, owed_int_in, operator, extend_times, cust_no, buss_rate, vouch_type, fine_intr_int, pay_back_acct, etl_dt, actu_buss_rate, putout_acct, fine_pr_int, mgr_no, pay_times, loan_use, dlay_days, actu_matu_date, occur_date, buss_type, mge_org, acct_no, dlay_amt, matu_date, norm_bal, dull_amt, bad_debt_amt, loan_channel, curr_type, uid, operate_org, buss_amt, putout_date, cust_name, due_intr_days, pay_type, ten_class, contract_no, pay_acct, src_dt, subject_no, loan_cust_no, intr_cyc, register) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public PreparedStatement insert(PreparedStatement preparedStatement) {
		try {
			preparedStatement.setString(1, reg_org);
			preparedStatement.setString(2, receipt_no);
			preparedStatement.setString(3, pay_cyc);
			preparedStatement.setString(4, intr_type);
			preparedStatement.setString(5, pay_freq);
			preparedStatement.setDouble(6, owed_int_out);
			preparedStatement.setDouble(7, bal);
			preparedStatement.setDouble(8, owed_int_in);
			preparedStatement.setString(9, operator);
			preparedStatement.setInt(10, extend_times);
			preparedStatement.setString(11, cust_no);
			preparedStatement.setDouble(12, buss_rate);
			preparedStatement.setString(13, vouch_type);
			preparedStatement.setDouble(14, fine_intr_int);
			preparedStatement.setString(15, pay_back_acct);
			preparedStatement.setString(16, etl_dt);
			preparedStatement.setDouble(17, actu_buss_rate);
			preparedStatement.setString(18, putout_acct);
			preparedStatement.setDouble(19, fine_pr_int);
			preparedStatement.setString(20, mgr_no);
			preparedStatement.setInt(21, pay_times);
			preparedStatement.setString(22, loan_use);
			preparedStatement.setInt(23, dlay_days);
			preparedStatement.setString(24, actu_matu_date);
			preparedStatement.setString(25, occur_date);
			preparedStatement.setString(26, buss_type);
			preparedStatement.setString(27, mge_org);
			preparedStatement.setString(28, acct_no);
			preparedStatement.setDouble(29, dlay_amt);
			preparedStatement.setString(30, matu_date);
			preparedStatement.setDouble(31, norm_bal);
			preparedStatement.setDouble(32, dull_amt);
			preparedStatement.setDouble(33, bad_debt_amt);
			preparedStatement.setString(34, loan_channel);
			preparedStatement.setString(35, curr_type);
			preparedStatement.setString(36, uid);
			preparedStatement.setString(37, operate_org);
			preparedStatement.setDouble(38, buss_amt);
			preparedStatement.setString(39, putout_date);
			preparedStatement.setString(40, cust_name);
			preparedStatement.setInt(41, due_intr_days);
			preparedStatement.setString(42, pay_type);
			preparedStatement.setString(43, ten_class);
			preparedStatement.setString(44, contract_no);
			preparedStatement.setString(45, pay_acct);
			preparedStatement.setString(46, src_dt);
			preparedStatement.setString(47, subject_no);
			preparedStatement.setString(48, loan_cust_no);
			preparedStatement.setString(49, intr_cyc);
			preparedStatement.setString(50, register);
			return preparedStatement;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return preparedStatement;
	}
}
