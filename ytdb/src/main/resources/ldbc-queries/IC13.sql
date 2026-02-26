SELECT shortestPath(
  (SELECT FROM Person WHERE id = :person1Id),
  (SELECT FROM Person WHERE id = :person2Id),
  'BOTH', 'knows'
).size() - 1 as pathLength
