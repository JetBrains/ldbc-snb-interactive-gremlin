package com.youtrackdb.ldbc.ytdb;

import com.youtrackdb.ldbc.ytdb.baseline.LdbcQuerySql;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

@Tag("profiling")
@ExtendWith(LdbcSnbDatabaseExtension.class)
class BaselineShortQuery4SqlTest {

  @Test
  void is4_sqlSelect(YTDBGraphTraversalSource g) throws Exception {
    LdbcQuerySqlProfilerSupport.profileSql(
        g,
        "IS4",
        LdbcQuerySql.IS4,
        Map.of("messageId", LdbcQuerySqlProfilerSupport.MESSAGE_ID));
  }
}
