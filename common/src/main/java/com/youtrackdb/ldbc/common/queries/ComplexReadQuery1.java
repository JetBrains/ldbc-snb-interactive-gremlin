package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery1;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery1Result;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.youtrackdb.ldbc.common.GremlinHelpers.*;
import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.Operator.sum;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

/**
 * IC1: Transitive friends with a certain name
 *
 * Given a start Person, find Persons with a given first name that the start Person is connected to
 * (excluding start Person) by at most 3 steps via the knows relationships. Return Persons, including
 * the distance (1..3), summaries of the Persons workplaces and places of study.
 */
public class ComplexReadQuery1 extends ListQueryHandler<LdbcQuery1, LdbcQuery1Result> {

    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcQuery1 operation, GraphTraversalSource g) {
        int maxHops = 3;
        int distance = 0;

        return g.withSack(distance).V()
                .has(PERSON, ID, operation.getPersonIdQ1())
                    .repeat(out(KNOWS).simplePath().sack(sum).by(constant(1))).times(maxHops).emit()
                .dedup()
                .has(FIRST_NAME, operation.getFirstName())
                .order()
                    .by(sack(), Order.asc)
                    .by(LAST_NAME, Order.asc)
                    .by(ID, Order.asc)
                .limit(operation.getLimit())
                .project(
                        ID, LAST_NAME, "distance", BIRTHDAY, CREATION_DATE,
                        GENDER, BROWSER_USED, LOCATION_IP, "emails", "languages",
                        "cityName", "universities", "companies"
                )
                    .by(ID)
                    .by(LAST_NAME)
                    .by(sack())
                    .by(BIRTHDAY)
                    .by(CREATION_DATE)
                    .by(GENDER)
                    .by(BROWSER_USED)
                    .by(LOCATION_IP)
                    .by(EMAILS)
                    .by(LANGUAGES)
                    .by(out(IS_LOCATED_IN).values(NAME))
                    .by(outE(STUDY_AT)
                            .project(NAME, CLASS_YEAR, "cityName")
                                    .by(inV().values(NAME))
                                    .by("classYear")
                                    .by(inV().out(IS_LOCATED_IN).values(NAME))
                            .fold())
                    .by(outE(WORK_AT)
                            .project(NAME, "year", "countryName")
                                    .by(inV().values(NAME))
                                    .by(WORK_FROM)
                                    .by(inV().out(IS_LOCATED_IN).values(NAME))
                            .fold());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected LdbcQuery1Result toResult(Map<String, Object> record) {
        long friendId = getLong(record, ID);
        String friendLastName = getString(record, LAST_NAME);
        int distanceFromPerson = getInt(record, "distance");
        long friendBirthday = getDateAsMillis(record, BIRTHDAY);
        long friendCreationDate = getDateAsMillis(record, CREATION_DATE);
        String friendGender = getString(record, GENDER);
        String friendBrowserUsed = getString(record, BROWSER_USED);
        String friendLocationIp = getString(record, LOCATION_IP);

        List<String> friendEmails = (List<String>) record.getOrDefault("emails", new ArrayList<>());
        List<String> friendLanguages = (List<String>) record.getOrDefault("languages", new ArrayList<>());
        String friendCityName = getString(record, "cityName");

        List<Map<String, Object>> univMaps = (List<Map<String, Object>>) record.getOrDefault("universities", new ArrayList<>());
        List<LdbcQuery1Result.Organization> universities = new ArrayList<>();
        for (Map<String, Object> org : univMaps) {
            universities.add(new LdbcQuery1Result.Organization(
                    (String) org.get(NAME),
                    ((Number) org.get(CLASS_YEAR)).intValue(),
                    (String) org.get("cityName")
            ));
        }

        List<Map<String, Object>> compMaps = (List<Map<String, Object>>) record.getOrDefault("companies", new ArrayList<>());
        List<LdbcQuery1Result.Organization> companies = new ArrayList<>();
        for (Map<String, Object> org : compMaps) {
            companies.add(new LdbcQuery1Result.Organization(
                    (String) org.get(NAME),
                    ((Number) org.get("year")).intValue(),
                    (String) org.get("countryName")
            ));
        }

        return new LdbcQuery1Result(
                friendId,
                friendLastName,
                distanceFromPerson,
                friendBirthday,
                friendCreationDate,
                friendGender,
                friendBrowserUsed,
                friendLocationIp,
                friendEmails,
                friendLanguages,
                friendCityName,
                universities,
                companies
        );
    }
}
