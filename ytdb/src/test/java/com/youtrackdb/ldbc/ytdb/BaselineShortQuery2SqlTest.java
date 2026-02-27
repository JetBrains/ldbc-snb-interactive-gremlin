package com.youtrackdb.ldbc.ytdb;

import com.youtrackdb.ldbc.ytdb.sql.LdbcQuerySql;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

@Tag("profiling")
@ExtendWith(LdbcSnbDatabaseExtension.class)
class SqlShortQuery2SqlTest {

  @Test
  void is2_sqlMatch(YTDBGraphTraversalSource g) throws Exception {
    LdbcQuerySqlProfilerSupport.profileSql(
        g,
        "IS2",
        LdbcQuerySql.IS2,
        Map.of("personId", LdbcQuerySqlProfilerSupport.PERSON_ID, "limit", 10));
  }
}
