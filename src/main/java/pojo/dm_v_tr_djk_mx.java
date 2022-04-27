package pojo;

import lombok.Data;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Data
public class dm_v_tr_djk_mx implements Insert {

	public String tran_date;

	public String tran_type;

	public String tran_desc;

	public String rev_ind;

	public String tran_time;

	public String pur_date;

	public String mer_type;

	public String val_date;

	public String uid;

	public String card_no;

	public String mer_code;

	public String acct_no;

	public String tran_type_desc;

	public String tran_amt_sign;

	public Double tran_amt = 0.0;

	public String etl_dt;

	@Override
	public PreparedStatement stat(Connection con) {
		try {
			return con.prepareStatement("INSERT INTO dm.dm_v_tr_djk_mx (tran_date, tran_type, tran_desc, rev_ind, tran_time, pur_date, mer_type, val_date, uid, card_no, mer_code, acct_no, tran_type_desc, tran_amt_sign, tran_amt, etl_dt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public PreparedStatement insert(PreparedStatement preparedStatement) {
		try {
			preparedStatement.setString(1, tran_date);
			preparedStatement.setString(2, tran_type);
			preparedStatement.setString(3, tran_desc);
			preparedStatement.setString(4, rev_ind);
			preparedStatement.setString(5, tran_time);
			preparedStatement.setString(6, pur_date);
			preparedStatement.setString(7, mer_type);
			preparedStatement.setString(8, val_date);
			preparedStatement.setString(9, uid);
			preparedStatement.setString(10, card_no);
			preparedStatement.setString(11, mer_code);
			preparedStatement.setString(12, acct_no);
			preparedStatement.setString(13, tran_type_desc);
			preparedStatement.setString(14, tran_amt_sign);
			preparedStatement.setDouble(15, tran_amt);
			preparedStatement.setString(16, etl_dt);
			return preparedStatement;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return preparedStatement;
	}
}
