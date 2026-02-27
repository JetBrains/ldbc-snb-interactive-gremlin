CREATE EDGE STUDY_AT FROM (SELECT FROM Person WHERE id = :personId)
  TO (SELECT FROM Organisation WHERE id = :orgId) SET classYear = :classYear