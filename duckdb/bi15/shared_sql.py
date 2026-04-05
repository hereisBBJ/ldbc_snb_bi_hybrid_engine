"""
bi15/shared_sql.py
BI-15 公共 SQL 片段，供不同后端复用。
"""

# 非迭代 SQL：子图过滤 + 匹配 + 边权计算，输出 (src, dst, weight)
EDGE_SELECT_SQL = """\
with
myForums(id) as (
    select id from Forum f
    where f.creationDate between getvariable('startDate') and getvariable('endDate')
),
mm as (
    select least(msg.CreatorPersonId, reply.CreatorPersonId)    as src,
           greatest(msg.CreatorPersonId, reply.CreatorPersonId) as dst,
           sum(case when msg.ParentMessageId is null then 10 else 5 end) as w
    from undirected_Person_knows_Person pp, Message msg, Message reply
    where pp.person1id = msg.CreatorPersonId
      and pp.person2id = reply.CreatorPersonId
      and reply.ParentMessageId = msg.MessageId
      and exists (select * from myForums f where f.id = msg.containerforumid)
      and exists (select * from myForums f where f.id = reply.containerforumid)
    group by src, dst
)
select pp.person1id as src,
       pp.person2id as dst,
       10.0 / (coalesce(mm.w, 0) + 10) as weight
from undirected_Person_knows_Person pp
left join mm
       on least(pp.person1id, pp.person2id)    = mm.src
      and greatest(pp.person1id, pp.person2id) = mm.dst
where pp.person1id < pp.person2id
"""
