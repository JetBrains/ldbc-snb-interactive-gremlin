package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate1AddPerson;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.V;

/**
 * Update 1: Add person
 *
 * Add a Person node, connected to the network by 4 possible edge types (isLocatedIn, hasInterest,
 * studyAt, workAt).
 */
public class Update1AddPerson extends UpdateHandler<LdbcUpdate1AddPerson> {

    @Override
    protected void executeUpdate(LdbcUpdate1AddPerson operation, GraphTraversalSource g) {
        var traversal = g.addV(PERSON)
                        .property(ID, operation.getPersonId())
                        .property(FIRST_NAME, operation.getPersonFirstName())
                        .property(LAST_NAME, operation.getPersonLastName())
                        .property(GENDER, operation.getGender())
                        .property(BIRTHDAY, operation.getBirthday())
                        .property(CREATION_DATE, operation.getCreationDate())
                        .property(LOCATION_IP, operation.getLocationIp())
                        .property(BROWSER_USED, operation.getBrowserUsed())
                        .property(LANGUAGES, operation.getLanguages())
                        .property(EMAILS, operation.getEmails())
                        .as("person")
                        .addE(IS_LOCATED_IN)
                        .from("person")
                        .to(V().has(PLACE, ID, operation.getCityId()));

                for (Long tagId : operation.getTagIds()) {
                    traversal.addE(HAS_INTEREST).from("person").to(V().has(TAG, ID, tagId));
                }

                for (LdbcUpdate1AddPerson.Organization org : operation.getStudyAt()) {
                    traversal.addE(STUDY_AT)
                            .from("person")
                            .to(V().has(UNIVERSITY, ID, org.getOrganizationId()))
                            .property(CLASS_YEAR, org.getYear());
                }

                for (LdbcUpdate1AddPerson.Organization org : operation.getWorkAt()) {
                    traversal.addE(WORK_AT)
                            .from("person")
                            .to(V().has(COMPANY, ID, org.getOrganizationId()))
                            .property(WORK_FROM, org.getYear());
                }

        traversal.iterate();
    }
}
