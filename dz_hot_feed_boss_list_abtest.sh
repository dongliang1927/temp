#!/bin/bash

if [ "$#" == "3"  ]
then
    date=$1
    date1=$1
    date2=$2
    date3=$3
else
    date=$(date -d '-0 day' +"%Y-%m-%d")
    date1=$(date -d '-0 day' +"%Y-%m-%d")
    date2=$(date -d '-0 day' +"%Y%m%d")
    date3=$(date -d '-3 day' +"%Y-%m-%d")
fi
    echo $date
    echo $date1
    echo $date2
    echo $date3

dir=/data1/dianzhangbi/dongliang/lixinghua/data
export JAVA_HOME=/usr/local/java8
HIVE=/usr/bin/hive
python=/usr/bin/python
mysql=/data/service/server/mysql/bin/mysql

source /data1/dianzhangbi/shellfunc/base_func.sh

echo =============================================每日要喂的火爆职位用户===============================================================================================

$HIVE -e"
use dzphoenix;insert overwrite table dz_vip_feed_boss_list_abtest partition (ds = '${date}')
select 
distinct a.bossid,a.jobid,a.jobcode,a.lat,a.lng,a.city_code,b.batchid
from
( ---------火爆职位桶用户
select  
distinct a.id as bossid,b.id as jobid,b.code as jobcode,a.lat,a.lng,c.city_code 
from 
dianzhang.dz_boss a 
inner join dianzhang.dz_job b on a.id = b.user_id
inner join dianzhang.dz_user c on a.id = c.id
left join dianzhang.original_blue_pack d  on b.code=d.job_code and b.city_code=d.city_code
where d.type=102
and a.approve_status = 1 ---认证
and b.status = 0 ---当前有效
)a 
inner join 
(
select
tt.user_id as uid,
'${date2}' as batchid
from (select------------当前火爆职位有效
c.user_id
from dianzhang.original_order_item c
inner join ods_dianzhang.ods_blue_job_boom m ON c.product_id=m.job_id
where m.boom_time>='${date1}')tt  
left join ( -------剔除会员未到期得人员
select 
distinct user_id as uid
from 
ods_dianzhang.blue_order_member
where status = 2  ----支付完成 
and date_add(substr(expire_time,1,10),cast(delay_days as int))>='${date1}' ----有效期>=今日 
)aa on tt.user_id=aa.uid
where aa.uid is null
)b on a.bossid = b.uid;"

# select count(distinct uid)
# from(
# select
# tt.user_id as uid
# from (select------------当前火爆职位有效
# c.user_id
# from dianzhang.original_order_item c
# inner join ods_dianzhang.ods_blue_job_boom m ON c.product_id=m.job_id
# where m.boom_time>='2020-01-09')tt  
# )s;
# 1151

# select count(distinct uid)
# from(
# select
# tt.user_id as uid
# from (select------------当前火爆职位有效
# c.user_id
# from dianzhang.original_order_item c
# inner join ods_dianzhang.ods_blue_job_boom m ON c.product_id=m.job_id
# where m.boom_time>='2020-01-09')tt  
# left join ( -------剔除会员未到期得人员
# select 
# distinct user_id as uid
# from 
# ods_dianzhang.blue_order_member
# where status = 2  ----支付完成 
# and date_add(substr(expire_time,1,10),cast(delay_days as int))>='2020-01-09' ----有效期>=今日 
# )aa on tt.user_id=aa.uid
# where aa.uid is null
# )s;
# 1014


# select count(distinct uid)
# from(
# select
# tt.user_id as uid
# from (select------------当前火爆职位有效
# c.user_id
# from dianzhang.original_order_item c
# inner join ods_dianzhang.ods_blue_job_boom m ON c.product_id=m.job_id
# where m.boom_time>='2020-01-09'
# and datediff(substr(m.boom_time,1,10),'2020-01-09')<=7 and datediff(substr(m.boom_time,1,10),'2020-01-09')>=0 )tt  
# left join ( -------剔除会员未到期得人员
# select 
# distinct user_id as uid
# from 
# ods_dianzhang.blue_order_member
# where status = 2  ----支付完成 
# and date_add(substr(expire_time,1,10),cast(delay_days as int))>='2020-01-09' ----有效期>=今日 
# )aa on tt.user_id=aa.uid
# where aa.uid is null
# )s;


