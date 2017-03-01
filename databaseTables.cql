CREATE KEYSPACE database_t WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':4};
CREATE TABLE database_t.test (
	hashtag varchar,
	location varchar,
	obscenityOverall varchar,
	obscenityPure varchar,
	emotion varchar,
	sentiment varchar,
	PRIMARY KEY(hashtag, location)
);

