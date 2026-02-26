MATCH {class: Person, as: start, where: (id = :personId)}
  .out('knows'){while: ($depth < 2), as: person}
  .in('HAS_CREATOR'){class: Post, as: post}
  .out('HAS_TAG'){as: givenTag, where: (name = :tagName)},
  {as: post}
  .out('HAS_TAG'){as: otherTag, where: (name <> :tagName)}
RETURN otherTag.name as tagName, count(*) as postCount
GROUP BY otherTag.name
ORDER BY postCount DESC, tagName ASC
LIMIT :limit
