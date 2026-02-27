CREATE EDGE LIKES FROM (SELECT FROM Person WHERE id = :personId)
  TO (SELECT FROM Post WHERE id = :postId) SET creationDate = :creationDate