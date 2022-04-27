create database if not exists dm;
use dm;
drop table if exists dm_v_tr_contract_mx;

CREATE TABLE dm.dm_v_tr_contract_mx
(
    uid              String,
    contract_no      Nullable(String),
    apply_no         Nullable(String),
    artificial_no    Nullable(String),
    occur_date       Nullable(String),
    loan_cust_no     Nullable(String),
    cust_name        Nullable(String),
    buss_type        Nullable(String),
    occur_type       Nullable(String),
    is_credit_cyc    Nullable(String),
    curr_type        Nullable(String),
    buss_amt         Nullable(Decimal(18, 2)),
    loan_pert        Nullable(Decimal(18, 2)),
    term_year        int,
    term_mth         int,
    term_day         int,
    base_rate_type   Nullable(String),
    base_rate        Decimal(18, 6),
    float_type       Nullable(String),
    rate_float       Decimal(18, 6),
    rate             Decimal(18, 6),
    pay_times        int,
    pay_type         Nullable(String),
    direction        Nullable(String),
    loan_use         Nullable(String),
    pay_source       Nullable(String),
    putout_date      Nullable(String),
    matu_date        Nullable(String),
    vouch_type       Nullable(String),
    is_oth_vouch     Nullable(String),
    apply_type       Nullable(String),
    extend_times     int,
    actu_out_amt     Nullable(Decimal(18, 2)),
    bal              Nullable(Decimal(18, 2)),
    norm_bal         Nullable(Decimal(18, 2)),
    dlay_bal         Nullable(Decimal(18, 2)),
    dull_bal         Nullable(Decimal(18, 2)),
    owed_int_in      Nullable(Decimal(18, 2)),
    owed_int_out     Nullable(Decimal(18, 2)),
    fine_pr_int      Nullable(Decimal(18, 2)),
    fine_intr_int    Nullable(Decimal(18, 2)),
    dlay_days        int,
    five_class       Nullable(String),
    class_date       Nullable(String),
    mge_org          Nullable(String),
    mgr_no           Nullable(String),
    operate_org      Nullable(String),
    operator         Nullable(String),
    operate_date     Nullable(String),
    reg_org          Nullable(String),
    register         Nullable(String),
    reg_date         Nullable(String),
    inte_settle_type Nullable(String),
    is_bad           Nullable(String),
    frz_amt          Decimal(18, 0),
    con_crl_type     Nullable(String),
    shift_type       Nullable(String),
    due_intr_days    int,
    reson_type       Nullable(String),
    shift_bal        Decimal(18, 0),
    is_vc_vouch      Nullable(String),
    loan_use_add     Nullable(String),
    finsh_type       Nullable(String),
    finsh_date       Nullable(String),
    sts_flag         Nullable(String),
    src_dt           Nullable(String),
    etl_dt           Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY uid;

drop table if exists dm_v_tr_djk_mx;

CREATE TABLE dm.dm_v_tr_djk_mx
(
    uid            String,
    card_no        Nullable(String),
    tran_type      Nullable(String),
    tran_type_desc Nullable(String),
    tran_amt       Nullable(Decimal(18, 2)),
    tran_amt_sign  Nullable(String),
    mer_type       Nullable(String),
    mer_code       Nullable(String),
    rev_ind        Nullable(String),
    tran_desc      Nullable(String),
    tran_date      Nullable(String),
    val_date       Nullable(String),
    pur_date       Nullable(String),
    tran_time      Nullable(String),
    acct_no        Nullable(String),
    etl_dt         Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY uid;

drop table if exists dm_v_tr_dsf_mx;

CREATE TABLE dm.dm_v_tr_dsf_mx
(
    tran_date       String,
    tran_log_no     Nullable(String),
    tran_code       Nullable(String),
    channel_flg     Nullable(String),
    tran_org        Nullable(String),
    tran_teller_no  Nullable(String),
    dc_flag         Nullable(String),
    tran_amt        Nullable(Decimal(18, 2)),
    send_bank       Nullable(String),
    payer_open_bank Nullable(String),
    payer_acct_no   Nullable(String),
    payer_name      Nullable(String),
    payee_open_bank Nullable(String),
    payee_acct_no   Nullable(String),
    payee_name      Nullable(String),
    tran_sts        Nullable(String),
    busi_type       Nullable(String),
    busi_sub_type   Nullable(String),
    etl_dt          Nullable(String),
    uid             Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY tran_date;

drop table if exists dm_v_tr_duebill_mx;

CREATE TABLE dm.dm_v_tr_duebill_mx
(
    uid            String,
    acct_no        Nullable(String),
    receipt_no     Nullable(String),
    contract_no    Nullable(String),
    subject_no     Nullable(String),
    cust_no        Nullable(String),
    loan_cust_no   Nullable(String),
    cust_name      Nullable(String),
    buss_type      Nullable(String),
    curr_type      Nullable(String),
    buss_amt       Nullable(Decimal(18, 2)),
    putout_date    Nullable(String),
    matu_date      Nullable(String),
    actu_matu_date Nullable(String),
    buss_rate      Decimal(31, 10),
    actu_buss_rate Decimal(31, 10),
    intr_type      Nullable(String),
    intr_cyc       Nullable(String),
    pay_times      int,
    pay_cyc        Nullable(String),
    extend_times   int,
    bal            Nullable(Decimal(18, 2)),
    norm_bal       Nullable(Decimal(18, 2)),
    dlay_amt       Nullable(Decimal(18, 2)),
    dull_amt       Decimal(31, 10),
    bad_debt_amt   Decimal(31, 10),
    owed_int_in    Nullable(Decimal(18, 2)),
    owed_int_out   Nullable(Decimal(18, 2)),
    fine_pr_int    Nullable(Decimal(18, 2)),
    fine_intr_int  Nullable(Decimal(18, 2)),
    dlay_days      int,
    pay_acct       Nullable(String),
    putout_acct    Nullable(String),
    pay_back_acct  Nullable(String),
    due_intr_days  int,
    operate_org    Nullable(String),
    operator       Nullable(String),
    reg_org        Nullable(String),
    register       Nullable(String),
    occur_date     Nullable(String),
    loan_use       Nullable(String),
    pay_type       Nullable(String),
    pay_freq       Nullable(String),
    vouch_type     Nullable(String),
    mgr_no         Nullable(String),
    mge_org        Nullable(String),
    loan_channel   Nullable(String),
    ten_class      Nullable(String),
    src_dt         Nullable(String),
    etl_dt         Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY uid;

drop table if exists dm_v_tr_etc_mx;

CREATE TABLE dm.dm_v_tr_etc_mx
(
    uid          String,
    etc_acct     Nullable(String),
    card_no      Nullable(String),
    car_no       Nullable(String),
    cust_name    Nullable(String),
    tran_date    Nullable(String),
    tran_time    Nullable(String),
    tran_amt_fen Nullable(Decimal(18, 2)),
    real_amt     Nullable(Decimal(18, 2)),
    conces_amt   Nullable(Decimal(18, 2)),
    tran_place   Nullable(String),
    mob_phone    Nullable(String),
    etl_dt       Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY uid;

drop table if exists dm_v_tr_grwy_mx;

CREATE TABLE dm.dm_v_tr_grwy_mx
(
    uid             String,
    mch_channel     Nullable(String),
    login_type      Nullable(String),
    ebank_cust_no   Nullable(String),
    tran_date       Nullable(String),
    tran_time       Nullable(String),
    tran_code       Nullable(String),
    tran_sts        Nullable(String),
    return_code     Nullable(String),
    return_msg      Nullable(String),
    sys_type        Nullable(String),
    payer_acct_no   Nullable(String),
    payer_acct_name Nullable(String),
    payee_acct_no   Nullable(String),
    payee_acct_name Nullable(String),
    tran_amt        Nullable(Decimal(18, 2)),
    etl_dt          Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY uid;

drop table if exists dm_v_tr_gzdf_mx;

CREATE TABLE dm.dm_v_tr_gzdf_mx
(
    belong_org   String,
    ent_acct     Nullable(String),
    ent_name     Nullable(String),
    eng_cert_no  Nullable(String),
    acct_no      Nullable(String),
    cust_name    Nullable(String),
    uid          Nullable(String),
    tran_date    Nullable(String),
    tran_amt     Nullable(Decimal(18, 2)),
    tran_log_no  Nullable(String),
    is_secu_card Nullable(String),
    trna_channel Nullable(String),
    batch_no     Nullable(String),
    etl_dt       Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY belong_org;

drop table if exists dm_v_tr_huanb_mx;

CREATE TABLE dm.dm_v_tr_huanb_mx
(
    tran_flag       String,
    uid             Nullable(String),
    cust_name       Nullable(String),
    acct_no         Nullable(String),
    tran_date       Nullable(String),
    tran_time       Nullable(String),
    tran_amt        Nullable(Decimal(18, 2)),
    bal             Nullable(Decimal(18, 2)),
    tran_code       Nullable(String),
    dr_cr_code      Nullable(String),
    pay_term        int,
    tran_teller_no  Nullable(String),
    pprd_rfn_amt    Nullable(Decimal(18, 2)),
    pprd_amotz_intr Nullable(Decimal(18, 2)),
    tran_log_no     Nullable(String),
    tran_type       Nullable(String),
    dscrp_code      Nullable(String),
    remark          Nullable(String),
    etl_dt          Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY tran_flag;

drop table if exists dm_v_tr_huanx_mx;

CREATE TABLE dm.dm_v_tr_huanx_mx
(
    tran_flag      String,
    uid            Nullable(String),
    cust_name      Nullable(String),
    acct_no        Nullable(String),
    tran_date      Nullable(String),
    tran_time      Nullable(String),
    tran_amt       Nullable(Decimal(18, 2)),
    cac_intc_pr    Nullable(Decimal(18, 2)),
    tran_code      Nullable(String),
    dr_cr_code     Nullable(String),
    pay_term       int,
    tran_teller_no Nullable(String),
    intc_strt_date Nullable(String),
    intc_end_date  Nullable(String),
    intr           Nullable(Decimal(18, 2)),
    tran_log_no    Nullable(String),
    tran_type      Nullable(String),
    dscrp_code     Nullable(String),
    etl_dt         Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY tran_flag;

drop table if exists dm_v_tr_sa_mx;

CREATE TABLE dm.dm_v_tr_sa_mx
(
    uid            String,
    card_no        Nullable(String),
    cust_name      Nullable(String),
    acct_no        Nullable(String),
    det_n          int,
    curr_type      Nullable(String),
    tran_teller_no Nullable(String),
    cr_amt         Nullable(Decimal(18, 2)),
    bal            Nullable(Decimal(18, 2)),
    tran_amt       Nullable(Decimal(18, 2)),
    tran_card_no   Nullable(String),
    tran_type      Nullable(String),
    tran_log_no    Nullable(String),
    dr_amt         Nullable(Decimal(18, 2)),
    open_org       Nullable(String),
    dscrp_code     Nullable(String),
    remark         Nullable(String),
    tran_time      Nullable(String),
    tran_date      String,
    sys_date       Nullable(String),
    tran_code      Nullable(String),
    remark_1       Nullable(String),
    oppo_cust_name Nullable(String),
    agt_cert_type  Nullable(String),
    agt_cert_no    Nullable(String),
    agt_cust_name  Nullable(String),
    channel_flag   Nullable(String),
    oppo_acct_no   Nullable(String),
    oppo_bank_no   Nullable(String),
    src_dt         Nullable(String),
    etl_dt         Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY uid
        PARTITION BY tran_date;

drop table if exists dm_v_tr_sbyb_mx;

CREATE TABLE dm.dm_v_tr_sbyb_mx
(
    uid            String,
    cust_name      Nullable(String),
    tran_date      Nullable(String),
    tran_sts       Nullable(String),
    tran_org       Nullable(String),
    tran_teller_no Nullable(String),
    tran_amt_fen   Nullable(Decimal(18, 2)),
    tran_type      Nullable(String),
    return_msg     Nullable(String),
    etl_dt         Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY uid;

drop table if exists dm_v_tr_sdrq_mx;

CREATE TABLE dm.dm_v_tr_sdrq_mx
(
    hosehld_no     String,
    acct_no        Nullable(String),
    cust_name      Nullable(String),
    tran_type      Nullable(String),
    tran_date      Nullable(String),
    tran_amt_fen   Nullable(Decimal(18, 2)),
    channel_flg    Nullable(String),
    tran_org       Nullable(String),
    tran_teller_no Nullable(String),
    tran_log_no    Nullable(String),
    batch_no       Nullable(String),
    tran_sts       Nullable(String),
    return_msg     Nullable(String),
    etl_dt         Nullable(String),
    uid            Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY hosehld_no;

drop table if exists dm_v_tr_sjyh_mx;

CREATE TABLE dm.dm_v_tr_sjyh_mx
(
    uid             String,
    mch_channel     Nullable(String),
    login_type      Nullable(String),
    ebank_cust_no   Nullable(String),
    tran_date       Nullable(String),
    tran_time       Nullable(String),
    tran_code       Nullable(String),
    tran_sts        Nullable(String),
    return_code     Nullable(String),
    return_msg      Nullable(String),
    sys_type        Nullable(String),
    payer_acct_no   Nullable(String),
    payer_acct_name Nullable(String),
    payee_acct_no   Nullable(String),
    payee_acct_name Nullable(String),
    tran_amt        Nullable(Decimal(18, 2)),
    etl_dt          Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY uid;

drop table if exists v_tr_shop_mx;

CREATE TABLE dm.v_tr_shop_mx
(
    tran_channel   String,
    order_code     Nullable(String),
    shop_code      Nullable(String),
    shop_name      Nullable(String),
    hlw_tran_type  Nullable(String),
    tran_date      Nullable(String),
    tran_time      Nullable(String),
    tran_amt       Nullable(Decimal(18, 2)),
    current_status Nullable(String),
    score_num      Nullable(Decimal(18, 2)),
    pay_channel    Nullable(String),
    uid            Nullable(String),
    legal_name     Nullable(String),
    etl_dt         Nullable(String)
)
    ENGINE = MergeTree
        ORDER BY tran_channel;

drop table if exists statistic_mx;

CREATE TABLE dm.statistic_mx
(
    tran_date      String,
    count          int
)
    ENGINE = MergeTree
        ORDER BY tran_date;
