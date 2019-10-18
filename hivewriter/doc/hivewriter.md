# DataX HiveWriter


---

## 1 快速介绍
````
导入数据到hive,可以自动进行分区

````
## 2 实现原理

````
手动分区灵活性太低，当分区数较多的时候一个个分区单独去加载数据的话工作量太大这时候可以考虑动态分区。
动态分区是基于hive中的源数据表将数据插入到分区表中，在数据插入的时候会根据分区字段自动将数据归类存入对应的分区路径，
而不需要手动指定分区路径。要使用动态分区必须要先开启动态分区:
SET hive.exec.dynamic.partition=true;  
SET hive.exec.dynamic.partition.mode=nonstrict; 
SET hive.exec.max.dynamic.partitions.pernode=1000; //动态分区的数量

1-把源数据读取的所有数据,写入到临时表. 通过复制表结构新建一个表:
create table mysql_to_hive_copy_02 like mysql_to_hive stored as orc LOCATION '/user/datax_writer_tmp/tmp001';
2-从临时表在插入目标表
insert into table page_view partition(dt) select * from original_page_view;
3-上面的步骤我们借助官方的hdfswriter来实现写入数据到hdfs
4-其实hdfswriter可以完成大部分的操作. 这里只是在其基础上面加一下小功能。
````

## 3 功能说明

### 3.1 配置样例

#### job.json

```
{
  "job": {
    "setting": {
      "speed": {
        "channel":1
      }
    },
    "content": [
      {
        "reader": {
          ...
        },
        "writer": {
          "name": "hivewriter",
          "parameter": {
            "databaseName":"default",
            "tableName": "test_topic",
            "defaultFS": "hdfs://xxx:port",
            "writeMode": "insert",
            "partition": [
                         "dt"
            ]
            "column": [
                          {
                            "name": "id",
                            "type": "INT"
                          },
                          {
                            "name": "username",
                            "type": "STRING"
                          },
                          {
                            "name": "telephone",
                            "type": "STRING"
                          },
                          {
                            "name": "mail",
                            "type": "STRING"
                          },
                          {
                             "name": "dt",
                             "type": "STRING"
                          }
                        ]
          }
        }
        }
    ]
  }

}
```

#### 3.2 参数说明
* **databaseName**

	* 描述：hive表所属库，默认是default

	* 必选：是 <br />

	* 默认值：无 <br />

* **tableName**

	* 描述：hive表名称

	* 必选：是 <br />

	* 默认值：无 <br />

* **defaultFS**

	* 描述：Hadoop hdfs文件系统namenode节点地址。格式：hdfs://ip:端口；例如：hdfs://127.0.0.1:9000<br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **writeMode**

	* 描述：写入数据的模式insert追加,overwrite 会先清空在导入新数据

	* 必选：是 <br />

	* 默认值：无 <br />
	
		
* **partition**

	* 描述：分区字段  ,  号分隔

	* 必选：是 <br />

	* 默认值：无 <br />
	
* **column**

	* 描述：暂时手动指定hive表列信息,按照建表的字段顺序列出来,分区信息也必须包含在列里面

	* 必选：是 <br />

	* 默认值：无 <br />	

#### 3.3 环境准备


## 4 性能报告

### 4.1 环境准备
- hadoop fs -mkdir /user/datax_writer_tmp


- 新建mysql表
````
CREATE TABLE `user_hive_mysql_day` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `username` varchar(50) DEFAULT NULL,
  `telephone` varchar(30) DEFAULT NULL,
  `mail` varchar(50) DEFAULT NULL,
  `day` varchar(8),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=48 DEFAULT CHARSET=utf8;

新建hive分区表
create table user_hive_mysql_day(
id int,
username string,
telephone string,
mail string
)
PARTITIONED BY(day string)
row format delimited fields terminated by '\t';



最后通过:
show partitions user_hive_mysql_day;
查看表里面有多少分区
````

#### 4.1.3 DataX jvm 参数

-Xms1024m -Xmx1024m -XX:+HeapDumpOnOutOfMemoryError

### 4.2 测试报告



### 4.3 测试总结


## 5 约束限制

