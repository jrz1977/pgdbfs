drop table if exists pgdbfs_data;
drop table if exists pgdbfs;

create table pgdbfs (
       id bigint not null,
       mnt_pt varchar(256) not null,
       ino bigint not null,
       parentid bigint not null,
       name varchar(256) not null,
       size bigint not null,
       segment_len int not null,
       is_dir boolean not null default false,
       create_ts timestamptz default current_timestamp,
       update_ts timestamptz default current_timestamp,
       constraint pgdbfs_pk primary key(id),
       constraint pgdbfs_uk unique(mnt_pt, ino)
);

create table pgdbfs_data (
       id bigint not null primary key,
       fsid bigint not null,
       segment_no bigint not null,
       data bytea not null,
       constraint pgdbfs_data_fk foreign key(fsid) references pgdbfs(id) on delete cascade
);

create index mnt_pt_idx on pgdbfs(mnt_pt);

drop sequence ino_seq;
create sequence ino_seq;
drop sequence if exists fsid_seq;
create sequence fsid_seq;

insert into pgdbfs (id, mnt_pt, ino, parentid, name, size, segment_len, is_dir)
 values (
   (select nextval('fsid_seq')),
   '/tmp/pgdbfs',
   (select nextval('ino_seq')),
   0,
   'rz',
   4096,
   0,
   true
   );
