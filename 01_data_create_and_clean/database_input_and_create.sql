create database if not exists myhive;

use myhive;

create table myhive.courier_device(
    no int comment '序号',
    city_id string comment '城市ID',
    day int comment '天数',
    courier_id string comment '快递员ID哈希值',
    os_version string comment '系统版本',
    phone_mode string comment '手机型号'
)row format delimited fields terminated by ','
tblproperties ('skip.header.line.count'='1');

create table myhive.merchant_device(
    no int comment '序号',
    city_id string comment '城市ID',
    day int comment '天数',
    shop_id string comment '商家ID哈希值',
    beacon_id string comment '信标ID哈希值',
    manufacturer string comment '制造商',
    brand string comment '品牌',
    model string comment '型号'
)row format delimited fields terminated by ','
tblproperties ('skip.header.line.count'='1');

create table myhive.courier_report(
    no int comment '序号',
    city_id string comment '城市ID',
    day int comment '天数',
    courier_id string comment '快递员ID哈希值',
    shop_id string comment '商家ID哈希值',
    acceptance_hour int comment '快递员接受订单时间（小时）',
    acceptance_minute int comment '快递员接受订单时间（分钟）',
    acceptance_second int comment '快递员接受订单时间（秒）',
    arrier_hour int comment '快递员到达商户时间（小时）',
    arrier_minute int comment '快递员到达商户时间（分钟）',
    arrier_second int comment '快递员到达商户时间（秒）',
    pickup_hour int comment '快递员取货时间（小时）',
    pickup_minute int comment '快递员取货时间（分钟）',
    pickup_second int comment '快递员取货时间（秒）',
    delivery_hour int comment '快递完成时间（小时）',
    delivery_minute int comment '快递完成时间（分钟）',
    delivery_second int comment '快递完成时间（秒）'
)row format delimited fields terminated by ','
tblproperties ('skip.header.line.count'='1');

create table myhive.courier_feedback(
    city_id string comment '城市ID',
    day int comment '天数',
    courier_id string comment '快递员ID哈希值',
    shop_id string comment '商家ID哈希值',
    acceptance_hour int comment '快递员接受订单时间（小时）',
    acceptance_minute int comment '快递员接受订单时间（分钟）',
    acceptance_second int comment '快递员接受订单时间（秒）',
    feedback_hour int comment '快递员响应通知时间（小时）',
    feedback_minute int comment '快递员响应通知时间（分钟）',
    feedback_second int comment '快递员响应通知时间（秒）',
    feedback int comment '快递员对通知的回应（0是稍后尝试，1是确认）',
    distance_to_merchant double comment '响应通知时快递员和商店的距离',
    gps_measure_offset int comment '反馈时间和gps时间的时间差'
)row format delimited fields terminated by ','
tblproperties ('skip.header.line.count'='1');


load data inpath '/tmp/file/Courier_device_data.csv' into table myhive.courier_device;

load data inpath '/tmp/file/Merchant_device_data.csv' into table myhive.merchant_device;

load data inpath '/tmp/file/Courier_feedback_data.csv/Courier_feedback_data_city_1.csv' into table myhive.courier_feedback;
load data inpath '/tmp/file/Courier_feedback_data.csv/Courier_feedback_data_city_2.csv' into table myhive.courier_feedback;
load data inpath '/tmp/file/Courier_feedback_data.csv/Courier_feedback_data_city_3.csv' into table myhive.courier_feedback;
load data inpath '/tmp/file/Courier_feedback_data.csv/Courier_feedback_data_city_4.csv' into table myhive.courier_feedback;
load data inpath '/tmp/file/Courier_feedback_data.csv/Courier_feedback_data_city_5.csv' into table myhive.courier_feedback;
load data inpath '/tmp/file/Courier_feedback_data.csv/Courier_feedback_data_city_6.csv' into table myhive.courier_feedback;
load data inpath '/tmp/file/Courier_feedback_data.csv/Courier_feedback_data_city_7.csv' into table myhive.courier_feedback;
load data inpath '/tmp/file/Courier_feedback_data.csv/Courier_feedback_data_city_8.csv' into table myhive.courier_feedback;

load data inpath '/tmp/file/Courier_report_data.csv/Courier_report_data_city_1.csv' into table myhive.courier_report;
load data inpath '/tmp/file/Courier_report_data.csv/Courier_report_data_city_2.csv' into table myhive.courier_report;
load data inpath '/tmp/file/Courier_report_data.csv/Courier_report_data_city_3.csv' into table myhive.courier_report;
load data inpath '/tmp/file/Courier_report_data.csv/Courier_report_data_city_4.csv' into table myhive.courier_report;
load data inpath '/tmp/file/Courier_report_data.csv/Courier_report_data_city_5.csv' into table myhive.courier_report;
load data inpath '/tmp/file/Courier_report_data.csv/Courier_report_data_city_6.csv' into table myhive.courier_report;
load data inpath '/tmp/file/Courier_report_data.csv/Courier_report_data_city_7.csv' into table myhive.courier_report;
load data inpath '/tmp/file/Courier_report_data.csv/Courier_report_data_city_8.csv' into table myhive.courier_report;
load data inpath '/tmp/file/Courier_report_data.csv/Courier_report_data_city_9.csv' into table myhive.courier_report;


-- create table myhive.courier_device_clean(
--     no int comment '序号',
--     city_id string comment '城市ID',
--     day int comment '天数',
--     courier_id string comment '快递员ID哈希值',
--     os_version string comment '系统版本',
--     phone_mode string comment '手机型号'
-- )row format delimited fields terminated by ',';
--
-- create table myhive.merchant_device_clean(
--     no int comment '序号',
--     city_id string comment '城市ID',
--     day int comment '天数',
--     shop_id string comment '商家ID哈希值',
--     beacon_id string comment '信标ID哈希值',
--     manufacturer string comment '制造商',
--     brand string comment '品牌',
--     model string comment '型号'
-- )row format delimited fields terminated by ',';
--
-- create table myhive.courier_report_clean(
--     no int comment '序号',
--     city_id string comment '城市ID',
--     day int comment '天数',
--     courier_id string comment '快递员ID哈希值',
--     shop_id string comment '商家ID哈希值',
--     acceptance_hour int comment '快递员接受订单时间（小时）',
--     acceptance_minute int comment '快递员接受订单时间（分钟）',
--     acceptance_second int comment '快递员接受订单时间（秒）',
--     arrier_hour int comment '快递员到达商户时间（小时）',
--     arrier_minute int comment '快递员到达商户时间（分钟）',
--     arrier_second int comment '快递员到达商户时间（秒）',
--     pickup_hour int comment '快递员取货时间（小时）',
--     pickup_minute int comment '快递员取货时间（分钟）',
--     pickup_second int comment '快递员取货时间（秒）',
--     delivery_hour int comment '快递完成时间（小时）',
--     delivery_minute int comment '快递完成时间（分钟）',
--     delivery_second int comment '快递完成时间（秒）'
-- )row format delimited fields terminated by ',';
--
-- create table myhive.courier_feedback_clean(
--     city_id string comment '城市ID',
--     day int comment '天数',
--     courier_id string comment '快递员ID哈希值',
--     shop_id string comment '商家ID哈希值',
--     acceptance_hour int comment '快递员接受订单时间（小时）',
--     acceptance_minute int comment '快递员接受订单时间（分钟）',
--     acceptance_second int comment '快递员接受订单时间（秒）',
--     feedback_hour int comment '快递员响应通知时间（小时）',
--     feedback_minute int comment '快递员响应通知时间（分钟）',
--     feedback_second int comment '快递员响应通知时间（秒）',
--     feedback int comment '快递员对通知的回应（0是稍后尝试，1是确认）',
--     distance_to_merchant double comment '响应通知时快递员和商店的距离',
--     gps_measure_offset int comment '反馈时间和gps时间的时间差'
-- )row format delimited fields terminated by ',';