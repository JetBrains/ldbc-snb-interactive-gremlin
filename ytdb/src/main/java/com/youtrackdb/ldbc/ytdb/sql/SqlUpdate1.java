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
      LdbcUpdate1AddPerson op,
      TinkerPopConnectionState state,
      ResultReporter rr) throws DbException {
    try {
      state.executeInTx(g -> {
        exec(g, LdbcQuerySql.U1,
            "personId", op.getPersonId(),
            "firstName", op.getPersonFirstName(),
            "lastName", op.getPersonLastName(),
            "gender", op.getGender(),
            "birthday", op.getBirthday(),
            "creationDate", op.getCreationDate(),
            "locationIP", op.getLocationIp(),
            "browserUsed", op.getBrowserUsed(),
            "languages", op.getLanguages(),
            "emails", op.getEmails(),
            "cityId", op.getCityId());

        for (Long tagId : op.getTagIds()) {
          exec(g, LdbcQuerySql.U1_HAS_INTEREST, "personId", op.getPersonId(), "tagId", tagId);
        }
        for (LdbcUpdate1AddPerson.Organization org : op.getStudyAt()) {
          exec(g, LdbcQuerySql.U1_STUDY_AT,
              "personId", op.getPersonId(),
              "orgId", org.getOrganizationId(),
              "classYear", org.getYear());
        }
        for (LdbcUpdate1AddPerson.Organization org : op.getWorkAt()) {
          exec(g, LdbcQuerySql.U1_WORK_AT,
              "personId", op.getPersonId(),
              "orgId", org.getOrganizationId(),
              "workFrom", org.getYear());
        }
      });
      rr.report(0, LdbcNoResult.INSTANCE, op);
    } catch (DbException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException("Error executing SQL Update 1", e);
    }
  }
}
