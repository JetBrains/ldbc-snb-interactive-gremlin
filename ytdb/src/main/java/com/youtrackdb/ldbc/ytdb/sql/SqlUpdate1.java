package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcNoResult;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate1AddPerson;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

/**
 * U1: Add Person.
 * Creates a Person vertex with IS_LOCATED_IN edge to a city,
 * plus variable-length HAS_INTEREST, STUDY_AT, and WORK_AT edges.
 */
public class SqlUpdate1
    implements OperationHandler<LdbcUpdate1AddPerson, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcUpdate1AddPerson operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      state.executeInTx(graph -> {
        exec(graph, LdbcQuerySql.U1,
            "personId", operation.getPersonId(),
            "firstName", operation.getPersonFirstName(),
            "lastName", operation.getPersonLastName(),
            "gender", operation.getGender(),
            "birthday", operation.getBirthday(),
            "creationDate", operation.getCreationDate(),
            "locationIP", operation.getLocationIp(),
            "browserUsed", operation.getBrowserUsed(),
            "languages", operation.getLanguages(),
            "emails", operation.getEmails(),
            "cityId", operation.getCityId());

        for (Long tagId : operation.getTagIds()) {
          exec(graph, LdbcQuerySql.U1_HAS_INTEREST, "personId", operation.getPersonId(), "tagId", tagId);
        }
        for (LdbcUpdate1AddPerson.Organization org : operation.getStudyAt()) {
          exec(graph, LdbcQuerySql.U1_STUDY_AT,
              "personId", operation.getPersonId(),
              "orgId", org.getOrganizationId(),
              "classYear", org.getYear());
        }
        for (LdbcUpdate1AddPerson.Organization org : operation.getWorkAt()) {
          exec(graph, LdbcQuerySql.U1_WORK_AT,
              "personId", operation.getPersonId(),
              "orgId", org.getOrganizationId(),
              "workFrom", org.getYear());
        }
      });
      resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
    } catch (Exception e) {
      throw new DbException("Error executing SQL Update 1", e);
    }
  }
}
