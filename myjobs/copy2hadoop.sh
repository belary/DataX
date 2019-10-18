#!/bin/bash

timestamp=`date +%Y%m%d%H -d "-1hours"`;
#ip=`ifconfig | grep 'inet ' |fgrep '10.100' | awk -F ' ' '{print $2}' | awk -F '.' '{print $4}'`
ip=$1
echo "IP:"${ip}
mkdir /mnt/data1/logs/crontab_log/ -p
log_file='/mnt/data1/logs/crontab_log/crontab_copy2hadoop.log'
hadoop_bin=/usr/hadoop/bin/hadoop
local_file_save_time=3

hdfs_path_suffix=`date +%Y/%m/%d -d "-1hours"`

function log(){
    echo $(date "+%Y-%m-%d %H:%M:%S")" $@" >> $log_file
    echo $(date "+%Y-%m-%d %H:%M:%S")" $@" 
}

function log_info(){
    log "[INFO] " "$@"
}

function log_error(){
    log "[ERROR] " "$@"
}


function copy_vip_file(){
	echo "start "$1
    app=$1
    hdfs_path="/vip/logs/${app}/$hdfs_path_suffix/"
    $hadoop_bin fs -mkdir -p "$hdfs_path" || log_error "mkdir hadoop path  ${app} ${timestamp} error"
    find /mnt/data1/heka/${app} -name "*$timestamp*log*" | xargs -i mv {} {}.${ip}
    find /mnt/data1/heka/${app} -name "*$timestamp*log*" -size +1 | xargs -i $hadoop_bin fs -copyFromLocal {} "${hdfs_path}"
    find /mnt/data1/heka/${app} -name "*log*" -mtime "+$local_file_save_time" | xargs -i rm -f {}
}
function copy_person_file(){
	echo "start "$1
    app=$1
    hdfs_path="/person/logs/${app}/$hdfs_path_suffix/"
    $hadoop_bin fs -mkdir -p "$hdfs_path" || log_error "mkdir hadoop path  ${app} ${timestamp} error"
    find /mnt/data1/heka/${app} -name "*$timestamp*log*" | xargs -i mv {} {}.${ip}
    find /mnt/data1/heka/${app} -name "*$timestamp*log*" -size +1 | xargs -i $hadoop_bin fs -copyFromLocal {} "${hdfs_path}"
    find /mnt/data1/heka/${app} -name "*log*" -mtime "+$local_file_save_time" | xargs -i rm -f {}
}

function copy_vip_nginx_file(){
	echo "start "$1
    app_local=$1
	app_hdfs=$2
    hdfs_path="/vip/logs/nginx/${app_hdfs}/$hdfs_path_suffix/"
    $hadoop_bin fs -mkdir -p "$hdfs_path" || log_error "mkdir hadoop path  ${app_hdfs} ${timestamp} error"
    find /mnt/data1/heka/vip-log/${app_local} -name "*$timestamp*log*" | xargs -i mv {} {}.${ip}
    find /mnt/data1/heka/vip-log/${app_local} -name "*$timestamp*log*" -size +1 | xargs -i $hadoop_bin fs -copyFromLocal {} "${hdfs_path}"
    find /mnt/data1/heka/vip-log/${app_local} -name "*log*" -mtime "+$local_file_save_time" | xargs -i rm -f {}
}

log_info "start copy2hdfs ${timestamp} ${ip}"
time1=`date "+%Y%m%d%H%M%S"`;

#copy_person_file 'courier'
#copy_person_file 'history'
#copy_person_file 'passport_session'
#copy_person_file 'dynamic'
#copy_person_file 'dynamic_nginx'
#copy_person_file 'homepage'
#copy_person_file 'galaxy'
#copy_person_file 'upgc'
#copy_person_file 'upgc_rc'
#copy_person_file 'person_log'
#copy_person_file 'tuyere'

#copy_vip_file 'passport'
#copy_vip_file 'billing_as'

copy_vip_nginx_file 'ott' 'ott'
copy_vip_nginx_file 'mpp' 'app'
copy_vip_nginx_file 'pc' 'pc'
copy_vip_nginx_file 'all' 'all'
copy_vip_nginx_file 'vip_bhv' 'bhv'
copy_vip_nginx_file 'buy_event' 'buy'
copy_vip_nginx_file 'js' 'js'
copy_vip_nginx_file 'vipact_event' 'vipact'
copy_vip_nginx_file 'flowshop' 'flowshop'
copy_vip_nginx_file 'dynamic_entry' 'dynamic'

copy_person_file 'tuyere'
copy_vip_file 'passport'
copy_vip_file 'billing_as'

#copy_tuyere_file
time2=`date "+%Y%m%d%H%M%S"`;
log_info "end  copy2hdfs ${timestamp} diff time: $(($time2-$time1))"
