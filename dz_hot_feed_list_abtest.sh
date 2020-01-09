#!/bin/bash

if [ "$#" == "4"  ]
then
    date=$1
    date1=$1
    date2=$2
    date3=$3
else
    date=$(date -d '-0 day' +"%Y-%m-%d") 
    date2=$(date -d '-1 day' +"%Y-%m-%d")  
    date3=$(date -d '-4 day' +"%Y-%m-%d")  
 date1=$(date -d '-1 day' +"%Y%m%d") 
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

echo =============================================每日火爆职位匹配用户 要喂的用户===============================================================================================

$HIVE -e"
use dzphoenix;
insert overwrite table dz_hot_feed_list_abtest_base partition(ds='${date}') 
select 
a.batchid,a.type,a.bossid,a.geekid,a.jobid,a.feedtime
from 
(
select 
'${date1}' as batchid,
case 
when datediff('${date}',substr(b.effect_time,1,10))<=7 and datediff('${date}',substr(b.effect_time,1,10))>=1 then 'begin'
when datediff(substr(b.boom_time,1,10),'${date}')<=7 and datediff(substr(b.boom_time,1,10),'${date}')>=0 then 'end'
else 'whole'
end as type,
bossid,geekid,jobid,
from_unixtime(unix_timestamp('${date} 00:00:00')+36000+cast(rand()*50400 as int))as feedtime
from 
dzphoenix.dz_hot_match_list_abtest a 

inner join 
(
select
tt.user_id as uid,
tt.effect_time,
tt.boom_time,
from (select------------当前火爆职位有效
c.user_id
from dianzhang.original_order_item c
inner join ods_dianzhang.ods_blue_job_boom m ON c.product_id=m.job_id
where m.boom_time>='${date}')tt  
left join ( -------剔除会员未到期得人员
select 
distinct user_id as uid
from 
ods_dianzhang.blue_order_member
where status = 2  ----支付完成 
and date_add(substr(expire_time,1,10),cast(delay_days as int))>='${date}' ----有效期>=今日 
)aa on tt.user_id=aa.uid
where aa.uid is null
)b on a.bossid = b.uid
and ds='${date2}')a;"

$HIVE -e"
use dzphoenix;
insert overwrite table dz_hot_feed_list_abtest partition(ds='${date}') 
select 
batchid,type,bossid,geekid,jobid,feedtime
from 
dzphoenix.dz_vip_feed_list_abtest_base
where ds='${date}' and type='end'
and substr(md5(bossid),32,1) in (0,1,2,3,4,5,6,7);"



