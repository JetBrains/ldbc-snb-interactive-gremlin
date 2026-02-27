CREATE VERTEX Comment SET id = :commentId, creationDate = :creationDate,
  locationIP = :locationIP, browserUsed = :browserUsed,
  content = :content, length = :length;
CREATE EDGE HAS_CREATOR FROM (SELECT FROM Comment WHERE id = :commentId)
  TO (SELECT FROM Person WHERE id = :authorPersonId);
CREATE EDGE IS_LOCATED_IN FROM (SELECT FROM Comment WHERE id = :commentId)
  TO (SELECT FROM Place WHERE id = :countryId)