\set ON_ERROR_STOP on

CREATE TABLE Organisation (
    id bigint PRIMARY KEY,
    type text NOT NULL,
    name text NOT NULL,
    url text NOT NULL,
    LocationPlaceId bigint NOT NULL
);

CREATE TABLE Place (
    id bigint PRIMARY KEY,
    name text NOT NULL,
    url text NOT NULL,
    type text NOT NULL,
    PartOfPlaceId bigint -- null for continents
);

CREATE TABLE Tag (
    id bigint PRIMARY KEY,
    name text NOT NULL,
    url text NOT NULL,
    TypeTagClassId bigint NOT NULL
);

CREATE TABLE TagClass (
    id bigint PRIMARY KEY,
    name text NOT NULL,
    url text NOT NULL,
    SubclassOfTagClassId bigint -- null for the root TagClass (Thing)
);

CREATE TABLE Country (
    id bigint primary key,
    name text not null,
    url text not null,
    PartOfContinentId bigint
);

CREATE TABLE City (
    id bigint primary key,
    name text not null,
    url text not null,
    PartOfCountryId bigint
);

CREATE TABLE Company (
    id bigint primary key,
    name text not null,
    url text not null,
    LocationPlaceId bigint not null
);

CREATE TABLE University (
    id bigint primary key,
    name text not null,
    url text not null,
    LocationPlaceId bigint not null
);

CREATE TABLE Comment (
    creationDate timestamp with time zone NOT NULL,
    id bigint NOT NULL, --PRIMARY KEY,
    locationIP text NOT NULL,
    browserUsed text NOT NULL,
    content text NOT NULL,
    length int NOT NULL,
    CreatorPersonId bigint NOT NULL,
    LocationCountryId bigint NOT NULL,
    ParentPostId bigint,
    ParentCommentId bigint
);

CREATE TABLE Forum (
    creationDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY,
    title text NOT NULL,
    ModeratorPersonId bigint -- can be null as its cardinality is 0..1
);

CREATE TABLE Post (
    creationDate timestamp with time zone NOT NULL,
    id bigint NOT NULL, --PRIMARY KEY,
    imageFile text,
    locationIP text NOT NULL,
    browserUsed text NOT NULL,
    language text,
    content text,
    length int NOT NULL,
    CreatorPersonId bigint NOT NULL,
    ContainerForumId bigint NOT NULL,
    LocationCountryId bigint NOT NULL
);

CREATE TABLE Person (
    creationDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY,
    firstName text NOT NULL,
    lastName text NOT NULL,
    gender text NOT NULL,
    birthday date NOT NULL,
    locationIP text NOT NULL,
    browserUsed text NOT NULL,
    LocationCityId bigint NOT NULL,
    speaks text NOT NULL,
    email text NOT NULL
);

CREATE TABLE Comment_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    CommentId bigint NOT NULL,
    TagId bigint NOT NULL
);

CREATE TABLE Post_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    PostId bigint NOT NULL,
    TagId bigint NOT NULL
);

CREATE TABLE Forum_hasMember_Person (
    creationDate timestamp with time zone NOT NULL,
    ForumId bigint NOT NULL,
    PersonId bigint NOT NULL
);

CREATE TABLE Forum_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    ForumId bigint NOT NULL,
    TagId bigint NOT NULL
);

CREATE TABLE Person_hasInterest_Tag (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    TagId bigint NOT NULL
);

CREATE TABLE Person_likes_Comment (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    CommentId bigint NOT NULL
);

CREATE TABLE Person_likes_Post (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    PostId bigint NOT NULL
);

CREATE TABLE Person_studyAt_University (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    UniversityId bigint NOT NULL,
    classYear int NOT NULL
);

CREATE TABLE Person_workAt_Company (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    CompanyId bigint NOT NULL,
    workFrom int NOT NULL
);

CREATE TABLE Person_knows_Person (
    creationDate timestamp with time zone NOT NULL,
    Person1id bigint NOT NULL,
    Person2id bigint NOT NULL,
    PRIMARY KEY (Person1id, Person2id)
);

CREATE TABLE Message (
    creationDate timestamp with time zone not null,
    MessageId bigint primary key,
    RootPostId bigint not null,
    RootPostLanguage text,
    content text,
    imageFile text,
    locationIP text not null,
    browserUsed text not null,
    length int not null,
    CreatorPersonId bigint not null,
    ContainerForumId bigint,
    LocationCountryId bigint not null,
    ParentMessageId bigint
);

CREATE TABLE Person_likes_Message (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    MessageId bigint NOT NULL
);

CREATE TABLE Message_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    MessageId bigint NOT NULL,
    TagId bigint NOT NULL
);

COPY Organisation FROM '/var/lib/postgresql/data/merge_postgre_SF10_data/merged_Organisation.csv' WITH CSV DELIMITER '|';
COPY Place FROM '/var/lib/postgresql/data/merge_postgre_SF10_data/merged_Place.csv' WITH CSV DELIMITER '|';
COPY Tag FROM '/var/lib/postgresql/data/merge_postgre_SF10_data/merged_Tag.csv' WITH CSV DELIMITER '|';
COPY TagClass FROM '/var/lib/postgresql/data/merge_postgre_SF10_data/merged_TagClass.csv' WITH CSV DELIMITER '|';
COPY Comment FROM '/var/lib/postgresql/data/merge_postgre_SF10_data/merged_Comment.csv' WITH CSV DELIMITER '|';
COPY Forum FROM '/var/lib/postgresql/data/merge_postgre_SF10_data/merged_Forum.csv' WITH CSV DELIMITER '|';
COPY Post FROM '/var/lib/postgresql/data/merge_postgre_SF10_data/merged_Post.csv' WITH CSV DELIMITER '|';
COPY Person FROM '/var/lib/postgresql/data/merge_postgre_SF10_data/merged_Person.csv' WITH CSV DELIMITER '|';
COPY Comment_hasTag_Tag FROM '/var/lib/postgresql/data/merge_postgre_SF10_data/merged_Comment_hasTag_Tag.csv' WITH CSV DELIMITER '|';
COPY Post_hasTag_Tag FROM '/var/lib/postgresql/data/merge_postgre_SF10_data/merged_Post_hasTag_Tag.csv' WITH CSV DELIMITER '|';
COPY Forum_hasMember_Person FROM '/var/lib/postgresql/data/merge_postgre_SF10_data/merged_Forum_hasMember_Person.csv' WITH CSV DELIMITER '|';
COPY Forum_hasTag_Tag FROM '/var/lib/postgresql/data/merge_postgre_SF10_data/merged_Forum_hasTag_Tag.csv' WITH CSV DELIMITER '|';
COPY Person_hasInterest_Tag FROM '/var/lib/postgresql/data/merge_postgre_SF10_data/merged_Person_hasInterest_Tag.csv' WITH CSV DELIMITER '|';
COPY Person_likes_Comment FROM '/var/lib/postgresql/data/merge_postgre_SF10_data/merged_Person_likes_Comment.csv' WITH CSV DELIMITER '|';
COPY Person_likes_Post FROM '/var/lib/postgresql/data/merge_postgre_SF10_data/merged_Person_likes_Post.csv' WITH CSV DELIMITER '|';
COPY Person_studyAt_University FROM '/var/lib/postgresql/data/merge_postgre_SF10_data/merged_Person_studyAt_University.csv' WITH CSV DELIMITER '|';
COPY Person_workAt_Company FROM '/var/lib/postgresql/data/merge_postgre_SF10_data/merged_Person_workAt_Company.csv' WITH CSV DELIMITER '|';
COPY Person_knows_Person FROM '/var/lib/postgresql/data/merge_postgre_SF10_data/merged_Person_knows_Person.csv' WITH CSV DELIMITER '|';

CREATE INDEX ON Tag (name);
CREATE INDEX ON Tag (TypeTagClassId);
CREATE INDEX ON person (LocationCityId);
CREATE INDEX ON person (id);
CREATE INDEX ON person (firstName);
CREATE INDEX ON person (lastName);
CREATE INDEX ON person (creationDate);
CREATE INDEX ON person (LocationCityId);
CREATE INDEX ON Person_hasInterest_Tag (PersonId);
CREATE INDEX ON Person_hasInterest_Tag (TagId);
CREATE INDEX ON Person_knows_Person (Person1Id);
CREATE INDEX ON Person_knows_Person (Person2Id);
CREATE INDEX ON Person_knows_Person (creationDate);
CREATE INDEX ON Forum (id);
CREATE INDEX ON Forum (title);
CREATE INDEX ON Forum (creationDate);
CREATE INDEX ON Forum (ModeratorPersonId);
CREATE INDEX ON Forum_hasMember_Person (ForumId);
CREATE INDEX ON Forum_hasMember_Person (PersonId);

INSERT INTO Country
    SELECT id, name, url, PartOfPlaceId AS PartOfContinentId
    FROM Place
    WHERE type = 'Country'
;

INSERT INTO City
    SELECT id, name, url, PartOfPlaceId AS PartOfCountryId
    FROM Place
    WHERE type = 'City'
;

INSERT INTO Company
    SELECT id, name, url, LocationPlaceId AS LocatedInCountryId
    FROM Organisation
    WHERE type = 'Company'
;

INSERT INTO University
    SELECT id, name, url, LocationPlaceId AS LocatedInCityId
    FROM Organisation
    WHERE type = 'University'
;

INSERT INTO Person_likes_Message
    SELECT creationDate, PersonId, PostId AS MessageId FROM Person_likes_Post
;

INSERT INTO Person_likes_Message
    SELECT creationDate, PersonId, CommentId AS MessageId FROM Person_likes_Comment
;

INSERT INTO Message_hasTag_Tag
    SELECT creationDate, PostId AS MessageId, TagId FROM Post_hasTag_Tag
;

INSERT INTO Message_hasTag_Tag
    SELECT creationDate, CommentId AS MessageId, TagId FROM Comment_hasTag_Tag
;

CREATE TABLE undirected_Person_knows_Person (
    creationDate timestamp with time zone NOT NULL,
    Person1id bigint NOT NULL,
    Person2id bigint NOT NULL
);
INSERT INTO undirected_Person_knows_Person SELECT * FROM Person_knows_Person;

INSERT INTO undirected_Person_knows_Person (creationDate, Person1id, Person2id)
SELECT Person_knows_Person.creationDate,
       Person_knows_Person.Person2Id AS Person1id,
       Person_knows_Person.Person1Id AS Person2id
FROM Person_knows_Person;

CREATE INDEX ON Person_likes_Message (PersonId);
CREATE INDEX ON Person_likes_Message (MessageId);
CREATE INDEX ON City (id);
CREATE INDEX ON City (PartOfCountryId);
CREATE INDEX ON Country (id);
CREATE INDEX ON Country (name);
CREATE INDEX ON undirected_Person_knows_Person (Person1Id);
CREATE INDEX ON undirected_Person_knows_Person (Person2Id);
CREATE INDEX ON undirected_Person_knows_Person (creationDate);

INSERT INTO Message
    SELECT
        creationDate,
        id AS MessageId,
        id AS RootPostId,
        language AS RootPostLanguage,
        content,
        imageFile,
        locationIP,
        browserUsed,
        length,
        CreatorPersonId,
        ContainerForumId,
        LocationCountryId,
        NULL::bigint AS ParentMessageId
    FROM Post
;

CREATE INDEX ON Comment (id);
CREATE INDEX ON Comment (ParentPostId);
CREATE INDEX ON Comment (ParentCommentId);
CREATE INDEX ON Comment (creationDate);
CREATE INDEX ON Comment (LocationCountryId);
CREATE INDEX ON Comment (content);
CREATE INDEX ON Comment (length);
CREATE INDEX ON Comment (CreatorPersonId);
CREATE INDEX ON Comment (locationIP);
CREATE INDEX ON Comment (browserUsed);

INSERT INTO Message
    WITH RECURSIVE Message_CTE(MessageId, RootPostId, RootPostLanguage, ContainerForumId, ParentMessageId) AS (
        SELECT
            Comment.id AS MessageId,
            Message.RootPostId AS RootPostId,
            Message.RootPostLanguage AS RootPostLanguage,
            Message.ContainerForumId AS ContainerForumId,
            coalesce(Comment.ParentPostId, Comment.ParentCommentId) AS ParentMessageId
        FROM Comment
        JOIN Message
          ON Message.MessageId = coalesce(Comment.ParentPostId, Comment.ParentCommentId)
        UNION ALL
        SELECT
            Comment.id AS MessageId,
            Message_CTE.RootPostId AS RootPostId,
            Message_CTE.RootPostLanguage AS RootPostLanguage,
            Message_CTE.ContainerForumId AS ContainerForumId,
            Comment.ParentCommentId AS ParentMessageId
        FROM Comment
        JOIN Message_CTE
          ON Comment.ParentCommentId = Message_CTE.MessageId
    )
    SELECT
        Comment.creationDate AS creationDate,
        Comment.id AS MessageId,
        Message_CTE.RootPostId AS RootPostId,
        Message_CTE.RootPostLanguage AS RootPostLanguage,
        Comment.content AS content,
        NULL::text AS imageFile,
        Comment.locationIP AS locationIP,
        Comment.browserUsed AS browserUsed,
        Comment.length AS length,
        Comment.CreatorPersonId AS CreatorPersonId,
        Message_CTE.ContainerForumId AS ContainerForumId,
        Comment.LocationCountryId AS LocationCityId,
        coalesce(Comment.ParentPostId, Comment.ParentCommentId) AS ParentMessageId
    FROM Message_CTE
    JOIN Comment
      ON Message_CTE.MessageId = Comment.id
;

CREATE INDEX ON Message (MessageId);
CREATE INDEX ON Message (CreatorPersonId);
CREATE INDEX ON Message (ParentMessageId);
CREATE INDEX ON Message (creationDate);
CREATE INDEX ON Message (length);
CREATE INDEX ON Message (RootPostLanguage);
CREATE INDEX ON Message (RootPostId);
CREATE INDEX ON Message (ContainerForumId);
CREATE INDEX ON Message (LocationCountryId);
CREATE INDEX ON Message_hasTag_Tag (MessageId);
CREATE INDEX ON Message_hasTag_Tag (TagId);

CREATE INDEX ON Forum_hasTag_Tag (ForumId);
CREATE INDEX ON Forum_hasTag_Tag (TagId);
CREATE INDEX ON University (LocationPlaceId);
CREATE INDEX ON Company (LocationPlaceId);
CREATE INDEX ON person (LocationCityId);
CREATE INDEX ON Person_workAt_Company (PersonId);
CREATE INDEX ON Person_workAt_Company (CompanyId);
CREATE INDEX ON Person_hasInterest_Tag (PersonId);
CREATE INDEX ON Person_hasInterest_Tag (TagId);
CREATE INDEX ON Person_studyAt_University (PersonId);
CREATE INDEX ON Person_studyAt_University (UniversityId);
CREATE INDEX ON TagClass (SubclassOfTagClassId);
