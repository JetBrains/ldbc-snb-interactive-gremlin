CREATE EDGE LIKES FROM (SELECT FROM Person WHERE id = :personId)
  TO (SELECT FROM Comment WHERE id = :commentId) SET creationDate = :creationDate