CREATE VERTEX Person SET id = :personId, firstName = :firstName, lastName = :lastName,
  gender = :gender, birthday = :birthday, creationDate = :creationDate,
  locationIP = :locationIP, browserUsed = :browserUsed,
  languages = :languages, emails = :emails;
CREATE EDGE IS_LOCATED_IN FROM (SELECT FROM Person WHERE id = :personId)
  TO (SELECT FROM Place WHERE id = :cityId)