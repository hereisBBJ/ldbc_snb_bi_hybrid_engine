/* Q11. Friend triangles
\set country '\'India\''
\set startDate '\'2012-09-29\''::timestamp
\set endDate '\'2013-01-01\''::timestamp
 */
WITH Persons_of_country_w_friends AS (
    SELECT Person.id AS PersonId
         , undirected_Person_knows_Person.Person2Id AS FriendId
         , undirected_Person_knows_Person.creationDate AS creationDate
      FROM Person
      JOIN City
        ON City.id = Person.LocationCityId
      JOIN Country
        ON Country.id = City.PartOfCountryId
       AND Country.name = :country
      JOIN undirected_Person_knows_Person
        ON undirected_Person_knows_Person.Person1Id = Person.id
)
SELECT count(*)
  FROM Persons_of_country_w_friends p1
  JOIN Persons_of_country_w_friends p2
    ON p1.FriendId = p2.PersonId
  JOIN Persons_of_country_w_friends p3
    ON p2.FriendId = p3.PersonId
   AND p3.FriendId = p1.PersonId
 WHERE true
    -- filter: unique triangles only
   AND p1.PersonId < p2.PersonId
   AND p2.PersonId < p3.PersonId
    -- filter: only edges created after :startDate
   AND :startDate <= p1.creationDate AND p1.creationDate <= :endDate
   AND :startDate <= p2.creationDate AND p2.creationDate <= :endDate
   AND :startDate <= p3.creationDate AND p3.creationDate <= :endDate
;
