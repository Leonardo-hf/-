use dm;
select (select count(1) from v_tr_shop_mx)        as shop,
       (select count(1) from dm_v_tr_sa_mx)       as sa,
       (select count(1) from dm_v_tr_sjyh_mx)     as sjyh,
       (select count(1) from dm_v_tr_sdrq_mx)     as sdrq,
       (select count(1) from dm_v_tr_sbyb_mx)     as sbyb,
       (select count(1) from dm_v_tr_huanx_mx)    as huanx,
       (select count(1) from dm_v_tr_huanb_mx)    as huanb,
       (select count(1) from dm_v_tr_etc_mx)      as etc,
       (select count(1) from dm_v_tr_gzdf_mx)     as gzdf,
       (select count(1) from dm_v_tr_duebill_mx)  as duebill,
       (select count(1) from dm_v_tr_grwy_mx)     as grwy,
       (select count(1) from dm_v_tr_dsf_mx)      as dsf,
       (select count(1) from dm_v_tr_djk_mx)      as djk,
       (select count(1) from dm_v_tr_contract_mx) as contract,
       shop + sa + sjyh + sdrq + sbyb + huanx + huanb + etc + gzdf + duebill + grwy + dsf + djk + contract
                                                  as count;