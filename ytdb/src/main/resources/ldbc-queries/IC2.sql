MATCH {class: Person, as: p, where: (id = :personId)}
  .out('knows'){as: friend}
  .in('HAS_CREATOR'){as: msg, where: (creationDate < :maxDate)}
RETURN friend.id as personId, friend.firstName, friend.lastName,
  msg.id as messageId,
  coalesce(msg.imageFile, msg.content) as messageContent,
  msg.creationDate as messageCreationDate
ORDER BY messageCreationDate DESC, messageId ASC
LIMIT :limit
