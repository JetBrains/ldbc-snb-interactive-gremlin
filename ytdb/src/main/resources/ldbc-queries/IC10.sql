SELECT personId, firstName, lastName,
  ($posScore[0].cnt - $negScore[0].cnt) as commonInterestScore,
  gender, birthday, cityName
FROM (
  MATCH {class: Person, as: start, where: (id = :personId)}
    .out('knows'){as: directFriend}
    .out('knows'){as: fof,
      where: ($currentMatch != $matched.start)},
    NOT {as: start}
      .out('knows'){as: fof}
  RETURN DISTINCT fof.id as personId, fof.firstName as firstName,
    fof.lastName as lastName, fof.gender as gender,
    fof.birthday as birthday,
    fof.out('IS_LOCATED_IN')[0].name as cityName,
    fof as fofVertex, start as startVertex
)
LET $tags = (
  SELECT set(out('HAS_INTEREST').@rid) as tagRids FROM Person
  WHERE @rid = $parent.$current.startVertex
),
$posScore = (
  SELECT count(*) as cnt FROM (
    SELECT expand(in('HAS_CREATOR')) FROM Person
    WHERE @rid = $parent.$current.fofVertex
  ) WHERE @class = 'Post'
    AND out('HAS_TAG').@rid CONTAINSANY $tags[0].tagRids
),
$negScore = (
  SELECT count(*) as cnt FROM (
    SELECT expand(in('HAS_CREATOR')) FROM Person
    WHERE @rid = $parent.$current.fofVertex
  ) WHERE @class = 'Post'
    AND NOT (out('HAS_TAG').@rid CONTAINSANY $tags[0].tagRids)
)
WHERE birthday >= :startDate AND birthday < :endDate
ORDER BY commonInterestScore DESC, personId ASC
LIMIT :limit
