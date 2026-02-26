MATCH {class: Person, as: start, where: (id = :personId)}
  .out('knows'){while: ($depth < 2), as: person}
  .inE('HAS_MEMBER'){as: membership, where: (joinDate > :minDate)}
  .outV(){class: Forum, as: forum}
  .out('CONTAINER_OF'){class: Post, as: post}
  .out('HAS_CREATOR'){as: postAuthor, where: (@rid = $matched.person.@rid)}
RETURN forum.title as forumTitle, forum.id as forumId, count(post) as postCount
GROUP BY forum.id, forum.title
ORDER BY postCount DESC, forumId ASC
LIMIT :limit
