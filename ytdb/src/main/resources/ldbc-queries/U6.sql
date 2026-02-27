CREATE VERTEX Post SET id = :postId, creationDate = :creationDate,
  locationIP = :locationIP, browserUsed = :browserUsed, language = :language,
  content = :content, imageFile = :imageFile, length = :length;
CREATE EDGE HAS_CREATOR FROM (SELECT FROM Post WHERE id = :postId)
  TO (SELECT FROM Person WHERE id = :authorPersonId);
CREATE EDGE CONTAINER_OF FROM (SELECT FROM Forum WHERE id = :forumId)
  TO (SELECT FROM Post WHERE id = :postId);
CREATE EDGE IS_LOCATED_IN FROM (SELECT FROM Post WHERE id = :postId)
  TO (SELECT FROM Place WHERE id = :countryId)