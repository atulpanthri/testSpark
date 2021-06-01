
create table feed_group(feed_grp_id serial primary key,group_name VARCHAR,team VARCHAR,database VARCHAR)

create table ds(ds_key serial primary key, ds_name VARCHAR)

create table Feed(Feed_id serial Primary key, Feed_name VARCHAR not null, feed_group INTEGER not null,ds_key integer not null)


create table Feed_attribute(
    attr_key serial primary key,
    attribute_name VARCHAR,
    ds_key  integer not null
)


create table feed_attribute_val(attr_val_key serial primary key,
                                attr_key INTEGER not null,
                                value VARCHAR,
                                feed_key INTEGER)

alter table feed_attribute_val add column feed_key INTEGER

select * from Feed_attribute


select * from feed
