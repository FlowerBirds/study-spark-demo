CREATE  TABLE `statics_custom_crowd`(
  `statics_pid` varchar(32) COMMENT '统计人群id',
  `statics_user_id` varchar(50)  COMMENT '创建人群的用户', 
  `statics_p_name` varchar(1000)  COMMENT '统计名称', 
  `statics_in_time_start` varchar(32)  COMMENT '统计纳入时间跨度start', 
  `statics_in_time_end` varchar(32)  COMMENT '统计纳入时间跨度end', 
  `statics_in_formula` varchar(4000)  COMMENT '统计纳入逻辑关系公式', 
  `statics_in_formula_new` varchar(4000)  COMMENT '统计纳入逻辑关系公式', 
  `static_field` varchar(3200)  COMMENT '随访地区', 
  `static_field_organ` string  COMMENT '随访地区的机构', 
  `static_year_span` varchar(10)  COMMENT '随访时间', 
  `static_year_time` varchar(11)  COMMENT '时间窗', 
  `delete_status` varchar(1)  COMMENT '删除状态', 
  `add_time` timestamp  COMMENT '创建时间', 
  `update_time` timestamp  COMMENT '更新时间', 
  `creator` varchar(32)  COMMENT '创建者', 
  `update_user` varchar(32)  COMMENT '更新者', 
  `publish` varchar(1)  COMMENT '发布状态', 
  `calc_status` varchar(2)  COMMENT '计算状态', 
  `statics_sum` varchar(32)  COMMENT '人群总数', 
  `type` varchar(2)  COMMENT '纳入类型', 
  `is_top` varchar(1)  COMMENT '是否是首页过滤人群', 
  `statics_out_formula` varchar(4000)  COMMENT '结局逻辑关系', 
  `statics_out_formula_new` varchar(4000)  COMMENT '统计纳入逻辑关系公式'
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