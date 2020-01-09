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
    date2=$(date -d '-1 day' +"%Y-%m-%d")
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


echo =============================================每日火爆职位匹配用户===============================================================================================

$HIVE -e"
use dzphoenix;
create temporary function distance as 'com.kanzhun.dianzhang.hive.udf.GeoDistance';
set hive.exec.parallel=true;
insert overwrite table dz_hot_match_list_abtest partition(ds='${date2}') 
select 
geekid,
code,
city_code,
jobid,
jobcode,
bossid,
num 
from 
(
select  
geekid,code,city_code,jobid,jobcode,bossid,num,
row_number() over(partition by geekid order by num) as geeknum 
from 
(
select 
geekid,code,city_code,jobid,jobcode,bossid,num,
row_number() over(partition by bossid,geekid order by  num) as jobnum  ----同一个BC，只喂1次
from 
(
select 
a.geekid,a.code,city_code,jobid,jobcode,a.bossid,rand(12345) as num  
from 

(
select 
distinct 
a.city_code,a.geekid,jobid,b.bossid,a.jobcode as code ,b.jobcode,
distance(a.lng,a.lat,b.lng,b.lat) as distan 
from dzphoenix.dz_hot_geek_feed_list_abtest a 
join dzphoenix.dz_hot_feed_boss_list_abtest b on a.city_code=b.city_code and a.jobcode=b.jobcode 
where a.ds='${date}'  
and b.ds='${date}' 
)a 
where distan<=30 -----距离小于30 
and not exists (select bossid,geekid from  dzphoenix.dz_add_info b where ds='${date2}' and a.bossid=b.bossid and a.geekid=b.geekid) ----好友关系表？
)a 
)a 
where jobnum=1 ----同一个BC，只喂1次
)a
where geeknum<=1;" 