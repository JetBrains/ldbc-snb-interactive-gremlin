SELECT tagName, postCount
FROM (
  SELECT tagName, count(postId) as postCount
  FROM (
    MATCH {class: Person, as: start, where: (id = :personId)}
      .out('KNOWS'){while: ($depth < 2), as: person,
        where: (@rid <> $matched.start.@rid)}
      .in('HAS_CREATOR'){class: Post, as: post}
      .out('HAS_TAG'){as: tag}
    RETURN DISTINCT post.id as postId, tag.name as tagName,
      post.out('HAS_TAG').name as postTags
  )
  WHERE tagName <> :tagName
    AND postTags CONTAINS :tagName
  GROUP BY tagName
)
ORDER BY postCount DESC, tagName COLLATE default
LIMIT :limit
