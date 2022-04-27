package pojo;

import lombok.Data;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Data
public class dm_v_tr_contract_mx implements Insert {

	public String reg_org;

	public String loan_use_add;

	public String artificial_no;

	public Double loan_pert = 0.0;

	public Double dull_bal = 0.0;

	public String finsh_date;

	public Double owed_int_out = 0.0;

	public Double bal = 0.0;

	public Double owed_int_in = 0.0;

	public String operator;

	public Integer extend_times = 0;

	public Double frz_amt = 0.0;

	public String shift_type;

	public String finsh_type;

	public Double shift_bal = 0.0;

	public String vouch_type;

	public Double fine_intr_int = 0.0;

	public String etl_dt;

	public String five_class;

	public String inte_settle_type;

	public Double fine_pr_int = 0.0;

	public String mgr_no;

	public Integer pay_times = 0;

	public String loan_use;

	public Integer dlay_days = 0;

	public String occur_date;

	public String reson_type;

	public String buss_type;

	public String reg_date;

	public Double base_rate = 0.0;

	public String is_oth_vouch;

	public String mge_org;

	public String occur_type;

	public String class_date;

	public String apply_type;

	public String matu_date;

	public Double norm_bal = 0.0;

	public String curr_type;

	public String uid;

	public Double rate_float = 0.0;

	public String operate_org;

	public Double actu_out_amt = 0.0;

	public Double rate = 0.0;

	public Double buss_amt = 0.0;

	public String putout_date;

	public String is_credit_cyc;

	public String float_type;

	public String cust_name;

	public String pay_type;

	public Integer due_intr_days = 0;

	public Integer term_day = 0;

	public Integer term_mth = 0;

	public String is_bad;

	public String con_crl_type;

	public String direction;

	public String contract_no;

	public Integer term_year = 0;

	public String operate_date;

	public String apply_no;

	public String pay_source;

	public String base_rate_type;

	public String is_vc_vouch;

	public String src_dt;

	public String sts_flag;

	public Double dlay_bal = 0.0;

	public String loan_cust_no;

	public String register;

	@Override
	public PreparedStatement stat(Connection con) {
		try {
			return con.prepareStatement("INSERT INTO dm.dm_v_tr_contract_mx (reg_org, loan_use_add, artificial_no, loan_pert, dull_bal, finsh_date, owed_int_out, bal, owed_int_in, operator, extend_times, frz_amt, shift_type, finsh_type, shift_bal, vouch_type, fine_intr_int, etl_dt, five_class, inte_settle_type, fine_pr_int, mgr_no, pay_times, loan_use, dlay_days, occur_date, reson_type, buss_type, reg_date, base_rate, is_oth_vouch, mge_org, occur_type, class_date, apply_type, matu_date, norm_bal, curr_type, uid, rate_float, operate_org, actu_out_amt, rate, buss_amt, putout_date, is_credit_cyc, float_type, cust_name, pay_type, due_intr_days, term_day, term_mth, is_bad, con_crl_type, direction, contract_no, term_year, operate_date, apply_no, pay_source, base_rate_type, is_vc_vouch, src_dt, sts_flag, dlay_bal, loan_cust_no, register) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public PreparedStatement insert(PreparedStatement preparedStatement) {
		try {
			preparedStatement.setString(1, reg_org);
			preparedStatement.setString(2, loan_use_add);
			preparedStatement.setString(3, artificial_no);
			preparedStatement.setDouble(4, loan_pert);
			preparedStatement.setDouble(5, dull_bal);
			preparedStatement.setString(6, finsh_date);
			preparedStatement.setDouble(7, owed_int_out);
			preparedStatement.setDouble(8, bal);
			preparedStatement.setDouble(9, owed_int_in);
			preparedStatement.setString(10, operator);
			preparedStatement.setInt(11, extend_times);
			preparedStatement.setDouble(12, frz_amt);
			preparedStatement.setString(13, shift_type);
			preparedStatement.setString(14, finsh_type);
			preparedStatement.setDouble(15, shift_bal);
			preparedStatement.setString(16, vouch_type);
			preparedStatement.setDouble(17, fine_intr_int);
			preparedStatement.setString(18, etl_dt);
			preparedStatement.setString(19, five_class);
			preparedStatement.setString(20, inte_settle_type);
			preparedStatement.setDouble(21, fine_pr_int);
			preparedStatement.setString(22, mgr_no);
			preparedStatement.setInt(23, pay_times);
			preparedStatement.setString(24, loan_use);
			preparedStatement.setInt(25, dlay_days);
			preparedStatement.setString(26, occur_date);
			preparedStatement.setString(27, reson_type);
			preparedStatement.setString(28, buss_type);
			preparedStatement.setString(29, reg_date);
			preparedStatement.setDouble(30, base_rate);
			preparedStatement.setString(31, is_oth_vouch);
			preparedStatement.setString(32, mge_org);
			preparedStatement.setString(33, occur_type);
			preparedStatement.setString(34, class_date);
			preparedStatement.setString(35, apply_type);
			preparedStatement.setString(36, matu_date);
			preparedStatement.setDouble(37, norm_bal);
			preparedStatement.setString(38, curr_type);
			preparedStatement.setString(39, uid);
			preparedStatement.setDouble(40, rate_float);
			preparedStatement.setString(41, operate_org);
			preparedStatement.setDouble(42, actu_out_amt);
			preparedStatement.setDouble(43, rate);
			preparedStatement.setDouble(44, buss_amt);
			preparedStatement.setString(45, putout_date);
			preparedStatement.setString(46, is_credit_cyc);
			preparedStatement.setString(47, float_type);
			preparedStatement.setString(48, cust_name);
			preparedStatement.setString(49, pay_type);
			preparedStatement.setInt(50, due_intr_days);
			preparedStatement.setInt(51, term_day);
			preparedStatement.setInt(52, term_mth);
			preparedStatement.setString(53, is_bad);
			preparedStatement.setString(54, con_crl_type);
			preparedStatement.setString(55, direction);
			preparedStatement.setString(56, contract_no);
			preparedStatement.setInt(57, term_year);
			preparedStatement.setString(58, operate_date);
			preparedStatement.setString(59, apply_no);
			preparedStatement.setString(60, pay_source);
			preparedStatement.setString(61, base_rate_type);
			preparedStatement.setString(62, is_vc_vouch);
			preparedStatement.setString(63, src_dt);
			preparedStatement.setString(64, sts_flag);
			preparedStatement.setDouble(65, dlay_bal);
			preparedStatement.setString(66, loan_cust_no);
			preparedStatement.setString(67, register);
			return preparedStatement;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return preparedStatement;
	}
}
