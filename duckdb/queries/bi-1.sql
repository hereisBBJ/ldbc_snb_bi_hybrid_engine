/* Q1. Posting summary
\set datetime '\'2011-12-01T00:00:00.000+00:00\''::timestamp
set variable datetime = TIMESTAMPTZ '2011-12-01 00:00:00.000+00:00';
 */
WITH
  message_count AS (
    SELECT 0.0 + count(*) AS cnt
      FROM Message
     WHERE creationDate < getvariable('datetime')
)
, message_prep AS (
    SELECT extract(year from creationDate) AS messageYear
         , ParentMessageId IS NOT NULL AS isComment
         , CASE
             WHEN length <  40 THEN 0 -- short
             WHEN length <  80 THEN 1 -- one liner
             WHEN length < 160 THEN 2 -- tweet
             ELSE                   3 -- long
           END AS lengthCategory
         , length
      FROM Message
     WHERE creationDate < getvariable('datetime')
       AND content IS NOT NULL
)
SELECT messageYear, isComment, lengthCategory
     , count(*) AS messageCount
     , avg(length::bigint) AS averageMessageLength
     , sum(length::bigint) AS sumMessageLength
     , count(*) / ANY_VALUE(mc.cnt) AS percentageOfMessages
  FROM message_prep
     , message_count mc
 GROUP BY messageYear, isComment, lengthCategory, mc.cnt
 ORDER BY messageYear DESC, isComment ASC, lengthCategory ASC
;
