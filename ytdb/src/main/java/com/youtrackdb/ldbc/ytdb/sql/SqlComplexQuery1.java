package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery1;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery1Result;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery1Result.Organization;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

/**
 * IC1: Transitive friends with certain name.
 * Returns friends up to 3 hops with a given firstName, including their
 * universities and companies as nested Organization lists.
 */
public class SqlComplexQuery1
    implements OperationHandler<LdbcQuery1, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcQuery1 operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      var results = state.computeInTx(graph -> {
        var rows = query(graph, LdbcQuerySql.IC1,
            "personId", operation.getPersonIdQ1(),
            "firstName", operation.getFirstName(),
            "limit", operation.getLimit());
        return rows.stream().map(row -> new LdbcQuery1Result(
            toLong(row.get("personId")),
            toStr(row.get("lastName")),
            toInt(row.get("distance")),
            toDateMillis(row.get("birthday")),
            toDateMillis(row.get("creationDate")),
            toStr(row.get("gender")),
            toStr(row.get("browserUsed")),
            toStr(row.get("locationIP")),
            toStringList(row.get("emails")),
            toStringList(row.get("languages")),
            toStr(row.get("cityName")),
            toOrganizations(row.get("universities"),
                "uniName", "classYear", "uniCityName"),
            toOrganizations(row.get("companies"),
                "compName", "workFromYear", "compCountryName")
        )).toList();
      });
      resultReporter.report(results.size(), results, operation);
    } catch (Exception e) {
      throw new DbException("Error executing SQL Complex Query 1", e);
    }
  }

  @SuppressWarnings("unchecked")
  private static List<Organization> toOrganizations(
      Object value, String nameKey, String yearKey, String placeKey) {
    if (value == null) return Collections.emptyList();
    if (value instanceof List<?> list) {
      List<Organization> orgs = new ArrayList<>();
      for (Object item : list) {
        if (item instanceof Map<?, ?> m) {
          orgs.add(new Organization(
              toStr(m.get(nameKey)),
              toInt(m.get(yearKey)),
              toStr(m.get(placeKey))));
        }
      }
      return orgs;
    }
    return Collections.emptyList();
  }
}
