\set ON_ERROR_STOP on

SET GLOBAL TimeZone = 'Etc/UTC';

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

COPY Organisation FROM '/work/bi-sf10-data/merged_Organisation.csv' WITH CSV DELIMITER '|';
COPY Place FROM '/work/bi-sf10-data/merged_Place.csv' WITH CSV DELIMITER '|';
COPY Tag FROM '/work/bi-sf10-data/merged_Tag.csv' WITH CSV DELIMITER '|';
COPY TagClass FROM '/work/bi-sf10-data/merged_TagClass.csv' WITH CSV DELIMITER '|';
COPY Comment FROM '/work/bi-sf10-data/merged_Comment.csv' WITH CSV DELIMITER '|';
COPY Forum FROM '/work/bi-sf10-data/merged_Forum.csv' WITH CSV DELIMITER '|';
COPY Post FROM '/work/bi-sf10-data/merged_Post.csv' WITH CSV DELIMITER '|';
COPY Person FROM '/work/bi-sf10-data/merged_Person.csv' WITH CSV DELIMITER '|';
COPY Comment_hasTag_Tag FROM '/work/bi-sf10-data/merged_Comment_hasTag_Tag.csv' WITH CSV DELIMITER '|';
COPY Post_hasTag_Tag FROM '/work/bi-sf10-data/merged_Post_hasTag_Tag.csv' WITH CSV DELIMITER '|';
COPY Forum_hasMember_Person FROM '/work/bi-sf10-data/merged_Forum_hasMember_Person.csv' WITH CSV DELIMITER '|';
COPY Forum_hasTag_Tag FROM '/work/bi-sf10-data/merged_Forum_hasTag_Tag.csv' WITH CSV DELIMITER '|';
COPY Person_hasInterest_Tag FROM '/work/bi-sf10-data/merged_Person_hasInterest_Tag.csv' WITH CSV DELIMITER '|';
COPY Person_likes_Comment FROM '/work/bi-sf10-data/merged_Person_likes_Comment.csv' WITH CSV DELIMITER '|';
COPY Person_likes_Post FROM '/work/bi-sf10-data/merged_Person_likes_Post.csv' WITH CSV DELIMITER '|';
COPY Person_studyAt_University FROM '/work/bi-sf10-data/merged_Person_studyAt_University.csv' WITH CSV DELIMITER '|';
COPY Person_workAt_Company FROM '/work/bi-sf10-data/merged_Person_workAt_Company.csv' WITH CSV DELIMITER '|';
COPY Person_knows_Person FROM '/work/bi-sf10-data/merged_Person_knows_Person.csv' WITH CSV DELIMITER '|';

CREATE INDEX idx_Tag_name ON Tag (name);
CREATE INDEX idx_Tag_TypeTagClassId ON Tag (TypeTagClassId);
CREATE INDEX idx_person_LocationCityId ON person (LocationCityId);
CREATE INDEX idx_person_id ON person (id);
CREATE INDEX idx_person_firstName ON person (firstName);
CREATE INDEX idx_person_lastName ON person (lastName);
CREATE INDEX idx_person_creationDate ON person (creationDate);
CREATE INDEX idx_person_LocationCityId ON person (LocationCityId);
CREATE INDEX idx_Person_hasInterest_Tag_PersonId ON Person_hasInterest_Tag (PersonId);
CREATE INDEX idx_Person_hasInterest_Tag_TagId ON Person_hasInterest_Tag (TagId);
CREATE INDEX idx_Person_knows_Person_Person1Id ON Person_knows_Person (Person1Id);
CREATE INDEX idx_Person_knows_Person_Person2Id ON Person_knows_Person (Person2Id);
CREATE INDEX idx_Person_knows_Person_creationDate ON Person_knows_Person (creationDate);
CREATE INDEX idx_Forum_id ON Forum (id);
CREATE INDEX idx_Forum_title ON Forum (title);
CREATE INDEX idx_Forum_creationDate ON Forum (creationDate);
CREATE INDEX idx_Forum_ModeratorPersonId ON Forum (ModeratorPersonId);
CREATE INDEX idx_Forum_hasMember_Person_ForumId ON Forum_hasMember_Person (ForumId);
CREATE INDEX idx_Forum_hasMember_Person_PersonId ON Forum_hasMember_Person (PersonId);

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

CREATE INDEX idx_Person_likes_Message_PersonId ON Person_likes_Message (PersonId);
CREATE INDEX idx_Person_likes_Message_MessageId ON Person_likes_Message (MessageId);
CREATE INDEX idx_City_id ON City (id);
CREATE INDEX idx_City_PartOfCountryId ON City (PartOfCountryId);
CREATE INDEX idx_Country_id ON Country (id);
CREATE INDEX idx_Country_name ON Country (name);
CREATE INDEX idx_undirected_Person_knows_Person_Person1Id ON undirected_Person_knows_Person (Person1Id);
CREATE INDEX idx_undirected_Person_knows_Person_Person2Id ON undirected_Person_knows_Person (Person2Id);
CREATE INDEX idx_undirected_Person_knows_Person_creationDate ON undirected_Person_knows_Person (creationDate);

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

CREATE INDEX idx_Comment_id ON Comment (id);
CREATE INDEX idx_Comment_ParentPostId ON Comment (ParentPostId);
CREATE INDEX idx_Comment_ParentCommentId ON Comment (ParentCommentId);
CREATE INDEX idx_Comment_creationDate ON Comment (creationDate);
CREATE INDEX idx_Comment_LocationCountryId ON Comment (LocationCountryId);
CREATE INDEX idx_Comment_content ON Comment (content);
CREATE INDEX idx_Comment_length ON Comment (length);
CREATE INDEX idx_Comment_CreatorPersonId ON Comment (CreatorPersonId);
CREATE INDEX idx_Comment_locationIP ON Comment (locationIP);
CREATE INDEX idx_Comment_browserUsed ON Comment (browserUsed);

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

CREATE INDEX idx_message_messageid ON Message (MessageId);
CREATE INDEX idx_message_creatorpersonid ON Message (CreatorPersonId);
CREATE INDEX idx_message_parentmessageid ON Message (ParentMessageId);
CREATE INDEX idx_message_creationdate ON Message (creationDate);
CREATE INDEX idx_Message_length ON Message (length);
CREATE INDEX idx_Message_RootPostLanguage ON Message (RootPostLanguage);
CREATE INDEX idx_Message_RootPostId ON Message (RootPostId);
CREATE INDEX idx_Message_ContainerForumId ON Message (ContainerForumId);
CREATE INDEX idx_Message_LocationCountryId ON Message (LocationCountryId);
CREATE INDEX idx_Message_hasTag_Tag_MessageId ON Message_hasTag_Tag (MessageId);
CREATE INDEX idx_Message_hasTag_Tag_TagId ON Message_hasTag_Tag (TagId);

CREATE INDEX idx_Forum_hasTag_Tag_ForumId ON Forum_hasTag_Tag (ForumId);
CREATE INDEX idx_Forum_hasTag_Tag_TagId ON Forum_hasTag_Tag (TagId);
CREATE INDEX idx_University_LocationPlaceId ON University (LocationPlaceId);
CREATE INDEX idx_Company_LocationPlaceId ON Company (LocationPlaceId);
CREATE INDEX idx_person_LocationCityId ON person (LocationCityId);
CREATE INDEX idx_Person_workAt_Company_PersonId ON Person_workAt_Company (PersonId);
CREATE INDEX idx_Person_workAt_Company_CompanyId ON Person_workAt_Company (CompanyId);
CREATE INDEX idx_Person_hasInterest_Tag_PersonId ON Person_hasInterest_Tag (PersonId);
CREATE INDEX idx_Person_hasInterest_Tag_TagId ON Person_hasInterest_Tag (TagId);
CREATE INDEX idx_Person_studyAt_University_PersonId ON Person_studyAt_University (PersonId);
CREATE INDEX idx_Person_studyAt_University_UniversityId ON Person_studyAt_University (UniversityId);
CREATE INDEX idx_TagClass_SubclassOfTagClassId ON TagClass (SubclassOfTagClassId);

