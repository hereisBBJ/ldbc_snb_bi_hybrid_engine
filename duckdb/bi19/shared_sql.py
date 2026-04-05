"""
bi19/shared_sql.py
Common SQL fragments for BI-19 graph extraction.
"""

# Full-mode edge construction: same non-iterative logic as queries/bi-19-full.sql.
EDGE_SELECT_SQL_FULL = """\
with
weights(src, dst, weight) as (
    select
        person1id as src,
        person2id as dst,
        greatest(round(40 - sqrt(count(*)))::bigint, 1)::double precision as weight
    from (select person1id, person2id from Person_knows_person where person1id < person2id) pp,
         Message m1,
         Message m2
    where pp.person1id = least(m1.creatorpersonid, m2.creatorpersonid)
      and pp.person2id = greatest(m1.creatorpersonid, m2.creatorpersonid)
      and m1.parentmessageid = m2.messageid
      and m1.creatorpersonid <> m2.creatorpersonid
    group by src, dst
)
select src, dst, weight
from weights
"""

# Precomputed-mode edge extraction: PathQ19 contains both directions, so collapse to unique undirected edges.
EDGE_SELECT_SQL_PRECOMPUTED = """\
select
    least(src, dst) as src,
    greatest(src, dst) as dst,
    min(w)::double precision as weight
from PathQ19
where src <> dst
group by 1, 2
"""

SRCS_SQL = "select id from Person where locationcityid = getvariable('city1Id')"
DSTS_SQL = "select id from Person where locationcityid = getvariable('city2Id')"
