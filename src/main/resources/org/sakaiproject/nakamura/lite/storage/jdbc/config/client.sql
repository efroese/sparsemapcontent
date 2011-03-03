delete-string-row = delete from css where rid = ?
delete-string-row.n.au = delete from au_css where rid = ?
delete-string-row.n.ac = delete from ac_css where rid = ?
delete-string-row.n.cn = delete from cn_css where rid = ?
select-string-row = select cid, v from css where rid = ?
select-string-row.n.au = select cid, v from au_css where rid = ?
select-string-row.n.ac = select cid, v from ac_css where rid = ?
select-string-row.n.cn = select cid, v from cn_css where rid = ?
insert-string-column = insert into css ( v, rid, cid) values ( ?, ?, ? )
insert-string-column.n.au = insert into au_css ( v, rid, cid) values ( ?, ?, ? )
insert-string-column.n.ac = insert into ac_css ( v, rid, cid) values ( ?, ?, ? )
insert-string-column.n.cn = insert into cn_css ( v, rid, cid) values ( ?, ?, ? )
update-string-column = update css set v = ?  where rid = ? and cid = ?
update-string-column.n.au = update au_css set v = ?  where rid = ? and cid = ?
update-string-column.n.ac = update ac_css set v = ?  where rid = ? and cid = ?
update-string-column.n.cn = update cn_css set v = ?  where rid = ? and cid = ?
remove-string-column = delete from css where rid = ? and cid = ?
remove-string-column.n.au = delete from au_css where rid = ? and cid = ?
remove-string-column.n.ac = delete from ac_css where rid = ? and cid = ?
remove-string-column.n.cn = delete from cn_css where rid = ? and cid = ?
check-schema = select count(*) from css

find.n.au = select a.rid, a.cid, a.v from au_css a {0} where {1} 1 = 1 ;, au_css {0} ; {0}.cid = ? and {0}.v = ? and {0}.rid = a.rid and
find.n.ac = select a.rid, a.cid, a.v from ac_css a {0} where {1} 1 = 1 ;, ac_css {0} ; {0}.cid = ? and {0}.v = ? and {0}.rid = a.rid and
find.n.cn = select a.rid, a.cid, a.v from cn_css a {0} where {1} 1 = 1 ;, cn_css {0} ; {0}.cid = ? and {0}.v = ? and {0}.rid = a.rid and
validate = values(1)
rowid-hash = SHA1


select-index-columns = select cid from index_cols


block-select-row = select b from css_b where rid = ?
block-delete-row = delete from css_b where rid = ?
block-insert-row = insert into css_b (rid,b) values (?, ?)
block-update-row = update css_b set b = ? where rid = ?

block-select-row.n.au = select b from au_css_b where rid = ?
block-delete-row.n.au = delete from au_css_b where rid = ?
block-insert-row.n.au = insert into au_css_b (rid,b) values (?, ?)
block-update-row.n.au = update au_css_b set b = ? where rid = ?

block-select-row.n.ac = select b from ac_css_b where rid = ?
block-delete-row.n.ac = delete from ac_css_b where rid = ?
block-insert-row.n.ac = insert into ac_css_b (rid,b) values (?, ?)
block-update-row.n.ac = update ac_css_b set b = ? where rid = ?

block-select-row.n.cn = select b from cn_css_b where rid = ?
block-delete-row.n.cn = delete from cn_css_b where rid = ?
block-insert-row.n.cn = insert into cn_css_b (rid,b) values (?, ?)
block-update-row.n.cn = update cn_css_b set b = ? where rid = ?


block-find = select a.rid, a.b from css_b a {0} where {1} 1 = 1;, css {0} ; {0}.cid = ? and {0}.v = ? and {0}.rid = a.rid and
block-find.n.au = select a.rid, a.b from au_css_b a {0} where {1} 1 = 1;, au_css {0} ; {0}.cid = ? and {0}.v = ? and {0}.rid = a.rid and
block-find.n.ac = select a.rid, a.b from ac_css_b a {0} where {1} 1 = 1;, ac_css {0} ; {0}.cid = ? and {0}.v = ? and {0}.rid = a.rid and
block-find.n.cn = select a.rid, a.b from cn_css_b a {0} where {1} 1 = 1;, cn_css {0} ; {0}.cid = ? and {0}.v = ? and {0}.rid = a.rid and

use-batch-inserts = 0
