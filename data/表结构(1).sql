CREATE  TABLE `statics_custom_crowd`(
  `statics_pid` varchar(32) NOT NULL COMMENT '统计人群id', 
  `statics_user_id` varchar(50) DEFAULT NULL COMMENT '创建人群的用户', 
  `statics_p_name` varchar(1000) DEFAULT NULL COMMENT '统计名称', 
  `statics_in_time_start` varchar(32) DEFAULT NULL COMMENT '统计纳入时间跨度start', 
  `statics_in_time_end` varchar(32) DEFAULT NULL COMMENT '统计纳入时间跨度end', 
  `statics_in_formula` varchar(4000) DEFAULT NULL COMMENT '统计纳入逻辑关系公式', 
  `statics_in_formula_new` varchar(4000) DEFAULT NULL COMMENT '统计纳入逻辑关系公式', 
  `static_field` varchar(3200) DEFAULT NULL COMMENT '随访地区', 
  `static_field_organ` string DEFAULT NULL COMMENT '随访地区的机构', 
  `static_year_span` varchar(10) DEFAULT NULL COMMENT '随访时间', 
  `static_year_time` varchar(11) DEFAULT NULL COMMENT '时间窗', 
  `delete_status` varchar(1) DEFAULT NULL COMMENT '删除状态', 
  `add_time` timestamp DEFAULT NULL COMMENT '创建时间', 
  `update_time` timestamp DEFAULT NULL COMMENT '更新时间', 
  `creator` varchar(32) DEFAULT NULL COMMENT '创建者', 
  `update_user` varchar(32) DEFAULT NULL COMMENT '更新者', 
  `publish` varchar(1) DEFAULT NULL COMMENT '发布状态', 
  `calc_status` varchar(2) DEFAULT NULL COMMENT '计算状态', 
  `statics_sum` varchar(32) DEFAULT NULL COMMENT '人群总数', 
  `type` varchar(2) DEFAULT NULL COMMENT '纳入类型', 
  `is_top` varchar(1) DEFAULT NULL COMMENT '是否是首页过滤人群', 
  `statics_out_formula` varchar(4000) DEFAULT NULL COMMENT '结局逻辑关系', 
  `statics_out_formula_new` varchar(4000) DEFAULT NULL COMMENT '统计纳入逻辑关系公式'
)
COMMENT '自定义统计人群'
CLUSTERED BY ( 
  `statics_pid`) 
INTO 3 BUCKETS
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
WITH SERDEPROPERTIES ( 
  'serialization.format'='1') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://nameservice1/inceptor1/user/hive/warehouse/ima_cdsp_test.db/hive/statics_custom_crowd'
TBLPROPERTIES (
  'orc.bloom.filter.columns'='statics_pid,statics_p_name,static_field,static_field_organ,static_year_span', 
  'last_load_time'='1599621479', 
  'COLUMN_STATS_ACCURATE'='false', 
  'transient_lastDdlTime'='1599621479', 
  'transactional'='true')
  ;
  
  
CREATE  TABLE `ima_cdsp_test.statics_custom_crowd_result`(
  `id` varchar(32) NOT NULL COMMENT '主键id', 
  `crowd_id` varchar(32) DEFAULT NULL COMMENT '统计人群id', 
  `source_id` varchar(32) DEFAULT NULL COMMENT '体检记录id', 
  `user_code` varchar(40) DEFAULT NULL COMMENT '体检人的唯一id', 
  `check_code` string DEFAULT NULL COMMENT '体检编号', 
  `check_date` varchar(40) DEFAULT NULL COMMENT '体检日期', 
  `institution_id` varchar(32) DEFAULT NULL COMMENT '机构id', 
  `type` varchar(10) DEFAULT NULL COMMENT '人群类型', 
  `update_time` varchar(50) DEFAULT NULL COMMENT '更新时间', 
  `show_number` varchar(5) DEFAULT NULL COMMENT '同个人下显示编号', 
  `diag_result` string DEFAULT NULL COMMENT '诊断结果'
)
COMMENT '人群结果表'
CLUSTERED BY ( 
  `id`) 
INTO 101 BUCKETS
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
WITH SERDEPROPERTIES ( 
  'serialization.format'='1') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://nameservice1/inceptor1/user/hive/warehouse/ima_cdsp_test.db/hive/statics_custom_crowd_result'
TBLPROPERTIES (
  'orc.bloom.filter.columns'='id,crowd_id,source_id,user_code,check_code,check_date,show_number,institution_id,type,diag_result', 
  'last_load_time'='1599621479', 
  'COLUMN_STATS_ACCURATE'='false', 
  'transient_lastDdlTime'='1599621479', 
  'transactional'='true')