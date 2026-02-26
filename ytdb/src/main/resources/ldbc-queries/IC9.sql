MATCH {class: Person, as: start, where: (id = :personId)}
  .out('knows'){while: ($depth < 2), as: person}
  .in('HAS_CREATOR'){as: msg, where: (creationDate < :maxDate)}
RETURN DISTINCT person.id as personId, person.firstName, person.lastName,
  msg.id as messageId,
  coalesce(msg.imageFile, msg.content) as messageContent,
  msg.creationDate as messageCreationDate
ORDER BY messageCreationDate DESC, messageId ASC
LIMIT :limit
