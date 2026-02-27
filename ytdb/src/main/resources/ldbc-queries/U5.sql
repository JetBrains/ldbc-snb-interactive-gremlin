CREATE EDGE HAS_MEMBER FROM (SELECT FROM Forum WHERE id = :forumId)
  TO (SELECT FROM Person WHERE id = :personId) SET joinDate = :joinDate