CREATE VERTEX Forum SET id = :forumId, title = :title, creationDate = :creationDate;
CREATE EDGE HAS_MODERATOR FROM (SELECT FROM Forum WHERE id = :forumId)
  TO (SELECT FROM Person WHERE id = :moderatorId)