insert into feed_group(group_name,team,database)
values ('IT','Ad','Ad')


insert into  ds (ds_name) values('file');
insert into  ds (ds_name) values('database');

insert into Feed(Feed_name, feed_group ,ds_key) values('ad',1,2);


select * from Feed_attribute

insert into Feed_attribute(attribute_name,ds_key) values('Source Path',1);
insert into Feed_attribute(attribute_name,ds_key) values('Destinination Path',1);
insert into Feed_attribute(attribute_name,ds_key) values('File Name',1);
insert into Feed_attribute(attribute_name,ds_key) values('FileType',1);
insert into Feed_attribute(attribute_name,ds_key) values('Field Separator',1);
insert into Feed_attribute(attribute_name,ds_key) values('Target Table Name',1);
insert into Feed_attribute(attribute_name,ds_key) values('Database Type',2);
insert into Feed_attribute(attribute_name,ds_key) values('Database Name',2);
insert into Feed_attribute(attribute_name,ds_key) values('Database Url',2);
insert into Feed_attribute(attribute_name,ds_key) values('Password',2);
insert into Feed_attribute(attribute_name,ds_key) values('User Name',2);
insert into Feed_attribute(attribute_name,ds_key) values('Driver',2);
insert into Feed_attribute(attribute_name,ds_key) values('Query',2);
insert into Feed_attribute(attribute_name,ds_key) values('Target Table Name',2);


insert into Feed_attribute(attribute_name,ds_key) values('Target File Name',2);
insert into Feed_attribute(attribute_name,ds_key) values('Target File Name',1);

insert into Feed_attribute(attribute_name,ds_key) values('Target File format',2);

select * from feed_attribute


select * from Feed_attribute

insert into feed_attribute_val(attr_key, value)   values(7,'postgres');
insert into feed_attribute_val(attr_key, value)   values(8,'postgres');
insert into feed_attribute_val(attr_key, value)   values(9,'jdbc:postgres://database-1.cfgcrzvguhx1.ap-southeast-1.rds.amazonaws.com');
insert into feed_attribute_val(attr_key, value)   values(10,'Etlpostgres');
insert into feed_attribute_val(attr_key, value)   values(11,'postgres');
insert into feed_attribute_val(attr_key, value)   values(12,'org.postgresql.Driver');
insert into feed_attribute_val(attr_key, value)   values(13,'Select * from emp');
insert into feed_attribute_val(attr_key, value)   values(14,'emp');

insert into feed_attribute_val(attr_key, value)   values(15,'emp');


insert into feed_attribute_val(attr_key, value) values(1,'D:\\spark\\testSpark\\data\\in\\employees');
insert into feed_attribute_val(attr_key, value) values(2,'D:\\spark\\testSpark\\data\\out\\staging');
insert into feed_attribute_val(attr_key, value) values(3,'part-00000.csv');
insert into feed_attribute_val(attr_key, value) values(4,'csv');
insert into feed_attribute_val(attr_key, value) values(5,'\\t');
insert into feed_attribute_val(attr_key, value) values(6,'emp');


insert into feed_attribute_val(attr_key, value,feed_key) values(15,'ad_emp',1);


select * from feed_attribute_val


update feed_attribute_val
 set feed_key = 2
where attr_key <=6

select * from Feed_attribute


select * from feed_attribute_val
where feed_key = 1

UPDATE feed_attribute_val
set value = 'org.postgresql.Driver'
where attr_val_key = 6


create 


select group_name , team, DATABASE, a.* from feed_group a

create view  vu_feed as 
select fv.feed_key, fg.group_name , fg.team, fg.DATABASE, f.Feed_name ,d.ds_name,fa.attribute_name,
fv.value
from feed f
 join  feed_group fg 
    on (fg.feed_grp_id = f.feed_group)
      join ds d 
       on (f.ds_Key= d.ds_key)  
        join feed_attribute_val fv
         on (f.feed_id= fv.feed_key )
            join Feed_attribute fa 
              on (fa.attr_key=fv.attr_key)


 where feed_id =1



 select * from vu_feed

 drop view vu_feed

 select * from feed_group

 update feed_group
 set "database" ='ad'