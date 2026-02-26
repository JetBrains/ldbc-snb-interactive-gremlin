SELECT * FROM (
  MATCH {class: Person, as: startPerson, where: (id = :personId)}
    .in('HAS_CREATOR'){as: message}
    .inE('LIKES'){as: likeEdge}
    .outV(){as: liker}
    .out('knows'){as: knowsStart, where: (@rid = $matched.startPerson.@rid), optional: true}
  RETURN liker.id as personId, liker.firstName, liker.lastName,
    max(likeEdge.creationDate) as latestLikeDate,
    likeEdge.creationDate as likeCreationDate,
    message.id as messageId,
    coalesce(message.imageFile, message.content) as messageContent,
    message.creationDate as messageCreationDate,
    ifnull(knowsStart, true, false) as isNew
  ORDER BY likeCreationDate DESC
)
GROUP BY personId
ORDER BY latestLikeDate DESC, personId ASC
LIMIT :limit
