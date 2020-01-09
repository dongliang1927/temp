#!/bin/bash

if [ "$#" == "3"  ]
then
    date=$1
    date1=$2
    date2=$3
    date3=$4
else
    date=$(date -d '-0 day' +"%Y-%m-%d")
    date1=$(date -d '-2 day' +"%Y-%m-%d")
    date2=$(date -d '-8 day' +"%Y-%m-%d")
    date3=$(date -d '-9 day' +"%Y-%m-%d")

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

echo =============================================每日新增开启闪电求职 和 新增C  喂B的C ===============================================================================================

hive -e"
use dianzhang;
insert overwrite table dzphoenix.dz_hot_geek_feed_list_abtest partition(ds = '${date}')
select 
distinct 
a.user_id as geekid,
c.code as jobcode,
b.lat,
b.lng,
b.city_code
from 
(
select  
ds,
user_id
from original_user_geek_extra a 
inner join dz_geek b on a.user_id = b.id 
where flush_helper rlike '1' 
and substr(a.update_time,1,10)=ds 
and ds >= '${date3}'
and b.hidden =0 ----非隐藏

union all 

select 
substr(comp_time,1,10)as ds,
id as user_id
from 
dz_geek 
where substr(comp_time,1,10)>= '${date3}'
and hidden =0 -----非隐藏

union all

select 
substr(comp_time,1,10)as ds,
id as user_id
from 
dianzhang.dz_geek a
inner join dianzhang.lhc_dianzhang_active_user b on a.id=b.uid
where hidden =0 and b.bg=1 and b.ds>='${date1}'
)a
inner join dianzhang.dz_geek b on a.user_id = b.id 
inner join dianzhang.dz_position c on a.user_id = c.user_id 
where c.type =1;"
