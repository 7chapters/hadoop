insert overwrite  directory '/user/cloudera/Review/hive/review_final'
select url_data.cat , url_data.url , url_data.pos ,
url_data.neg , url_data.total, url_data.hash
from 
(
select u.category as cat , r.positive as pos, r.url as url , 
r.negative as neg ,r.totalreview as total , r.hash as hash
from url_cat u 
join url_review_distinct r  on u.hash = r.hash 
)
url_data
join (
select u.category as cat , max(r.positive) as pos from url_cat u 
join url_review_distinct r  on u.hash = r.hash group by u.category having pos>0
) max_reivew 
on   max_reivew.pos = url_data.pos
where max_reivew.cat = url_data.cat
and url_data.total > 500
order by url_data.cat;