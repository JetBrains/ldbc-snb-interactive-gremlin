SELECT personId, lastName, distance, birthday, creationDate, gender,
  browserUsed, locationIP, emails, languages, cityName,
  $universities as universities, $companies as companies
FROM (
  MATCH {class: Person, as: start, where: (id = :personId)}
    .out('knows'){while: ($depth < 3), as: friend,
      where: (firstName = :firstName), depthAlias: dist}
    .out('IS_LOCATED_IN'){as: city}
  RETURN DISTINCT friend.id as personId, friend.lastName as lastName,
    dist as distance,
    friend.birthday as birthday, friend.creationDate as creationDate,
    friend.gender as gender,
    friend.browserUsed as browserUsed, friend.locationIP as locationIP,
    friend.email as emails, friend.languages as languages,
    city.name as cityName,
    friend as friendVertex
)
LET $universities = (
  SELECT classYear, uniName, uniCityName FROM (
    SELECT outE('STUDY_AT').classYear as classYear,
      outE('STUDY_AT').inV().name as uniName,
      outE('STUDY_AT').inV().out('IS_LOCATED_IN').name as uniCityName
    FROM Person WHERE @rid = $parent.current.friendVertex
  )
),
$companies = (
  SELECT workFromYear, compName, compCountryName FROM (
    SELECT outE('WORK_AT').workFrom as workFromYear,
      outE('WORK_AT').inV().name as compName,
      outE('WORK_AT').inV().out('IS_LOCATED_IN').name as compCountryName
    FROM Person WHERE @rid = $parent.current.friendVertex
  )
)
ORDER BY distance ASC, lastName ASC, personId ASC
LIMIT :limit
