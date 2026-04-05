"""
bi20/shared_sql.py
Common SQL fragments for BI-20 graph extraction.
"""

# Full-mode edge construction: same non-iterative logic as queries/bi-20-full.sql.
EDGE_SELECT_SQL_FULL = """\
select
    p1.personid as src,
    p2.personid as dst,
    (min(abs(p1.classYear - p2.classYear)) + 1)::double precision as weight
from undirected_Person_knows_person pp,
     Person_studyAt_University p1,
     Person_studyAt_University p2
where pp.person1id = p1.personid
  and pp.person2id = p2.personid
  and p1.universityid = p2.universityid
group by p1.personid, p2.personid
"""

# Precomputed-mode edge extraction.
EDGE_SELECT_SQL_PRECOMPUTED = """\
select
    src,
    dst,
    w::double precision as weight
from PathQ20
where src <> dst
"""

SRCS_SQL = "select getvariable('person2Id') as id"
DSTS_SQL = """\
select personid as id
from Person_workat_company pwc, Company c
where pwc.companyid = c.id and c.name = getvariable('company')
"""
