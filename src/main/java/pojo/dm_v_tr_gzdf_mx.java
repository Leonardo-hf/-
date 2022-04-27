package pojo;

import lombok.Data;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Data
public class dm_v_tr_gzdf_mx implements Insert {

	public String tran_date;

	public String batch_no;

	public String uid;

	public String is_secu_card;

	public String acct_no;

	public String belong_org;

	public String ent_acct;

	public String cust_name;

	public String ent_name;

	public String eng_cert_no;

	public Double tran_amt = 0.0;

	public String trna_channel;

	public String tran_log_no;

	public String etl_dt;

	@Override
	public PreparedStatement stat(Connection con) {
		try {
			return con.prepareStatement("INSERT INTO dm.dm_v_tr_gzdf_mx (tran_date, batch_no, uid, is_secu_card, acct_no, belong_org, ent_acct, cust_name, ent_name, eng_cert_no, tran_amt, trna_channel, tran_log_no, etl_dt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
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
			preparedStatement.setString(3, uid);
			preparedStatement.setString(4, is_secu_card);
			preparedStatement.setString(5, acct_no);
			preparedStatement.setString(6, belong_org);
			preparedStatement.setString(7, ent_acct);
			preparedStatement.setString(8, cust_name);
			preparedStatement.setString(9, ent_name);
			preparedStatement.setString(10, eng_cert_no);
			preparedStatement.setDouble(11, tran_amt);
			preparedStatement.setString(12, trna_channel);
			preparedStatement.setString(13, tran_log_no);
			preparedStatement.setString(14, etl_dt);
			return preparedStatement;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return preparedStatement;
	}
}
