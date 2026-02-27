CREATE EDGE KNOWS FROM (SELECT FROM Person WHERE id = :person1Id)
  TO (SELECT FROM Person WHERE id = :person2Id) SET creationDate = :creationDate;
CREATE EDGE KNOWS FROM (SELECT FROM Person WHERE id = :person2Id)
  TO (SELECT FROM Person WHERE id = :person1Id) SET creationDate = :creationDate