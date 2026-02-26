MATCH {class: Person, as: start, where: (id = :personId)}
  .out('knows'){while: ($depth < 2), as: person}
  .outE('WORK_AT'){as: workEdge, where: (workFrom < :workFromYear)}
  .inV(){as: company}
  .out('IS_LOCATED_IN'){as: country, where: (name = :countryName)}
RETURN person.id as personId, person.firstName, person.lastName,
  company.name as organizationName, workEdge.workFrom as organizationWorkFromYear
ORDER BY organizationWorkFromYear ASC, personId ASC, organizationName DESC
LIMIT :limit
