package com.youtrackdb.ldbc.ytdb;

import com.youtrackdb.ldbc.ytdb.sql.LdbcQuerySql;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

@Tag("profiling")
@ExtendWith(LdbcSnbDatabaseExtension.class)
class SqlShortQuery1SqlTest {

  @Test
  void is1_sqlMatch(YTDBGraphTraversalSource g) throws Exception {
    LdbcQuerySqlProfilerSupport.profileSql(
        g,
        "IS1",
        LdbcQuerySql.IS1,
        Map.of("personId", LdbcQuerySqlProfilerSupport.PERSON_ID));
  }
}
