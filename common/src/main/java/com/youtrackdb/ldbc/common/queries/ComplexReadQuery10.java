package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery10;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery10Result;

import java.util.Map;

import static com.youtrackdb.ldbc.common.GremlinHelpers.*;
import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.P.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

/**
 * IC10: Friend recommendation
 *
 * Given a start Person, find that Person's friends of friends (excluding the start Person and immediate
 * friends), who were born on or after the 21st of a given month and before the 22nd of the following
 * month. Calculate the similarity between each friend and the start person based on common interests:
 * commonInterestScore = (number of Posts with Tags the start person is interested in) - (number of
 * Posts with no such Tags).
 */
public class ComplexReadQuery10 extends ListQueryHandler<LdbcQuery10, LdbcQuery10Result> {

    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcQuery10 operation, GraphTraversalSource g) {
        int startMonth = operation.getMonth();
        int startDay = 21;
        int endMonth = startMonth == 12 ? 1 : startMonth + 1;
        int endDay = 22;
        String startMonthStr = String.format("%02d", startMonth);
        String startDayStr = String.format("%02d", startDay);
        String endMonthStr = String.format("%02d", endMonth);
        String endDayStr = String.format("%02d", endDay);


        return g.V()
                .has(PERSON, ID, operation.getPersonIdQ10()).as("start")
                    .out(HAS_INTEREST).aggregate("tags")
                .select("start").aggregate("exclude")
                    .out(KNOWS).dedup().aggregate("exclude")
                        .out(KNOWS).where(without("exclude"))
                .dedup()
                // Workaround: Extract month/day using substring on ISO-8601 string (e.g., "1980-08-17T00:00:00Z")
                // since Gremlin has no date component accessors and we cannot use lambdas (not serializable for remote execution).
                // asDate() ensures java.util.Date → OffsetDateTime → consistent string format
                .project("month", "day", "person")
                    .by(values("birthday").asDate().asString().substring(5, 7))
                    .by(values("birthday").asDate().asString().substring(8, 10))
                    .by(identity())
                .where(
                    or(
                        and(
                            select("month").is(startMonthStr),
                            select("day").is(gte(startDayStr))
                        ),
                        and(
                            select("month").is(endMonthStr),
                            select("day").is(lt(endDayStr))
                        )
                    )
                )
                .select("person")
                .as("person")
                .project("personId", FIRST_NAME, LAST_NAME,
                        "commonInterestScore", GENDER, "cityName")
                    .by(ID)
                    .by(FIRST_NAME)
                    .by(LAST_NAME)
                    .by(
                        project("common", "uncommon")
                            .by(in(HAS_CREATOR).hasLabel(POST).where(
                                    out(HAS_TAG).where(within("tags"))
                            ).count())
                            .by(in(HAS_CREATOR).hasLabel(POST).not(
                                    out(HAS_TAG).where(within("tags"))
                            ).count())
                            .math("common - uncommon")
                    )
                    .by(GENDER)
                    .by(out(IS_LOCATED_IN).values(NAME))
                .order()
                    .by("commonInterestScore", Order.desc)
                    .by("personId", Order.asc)
                .limit(operation.getLimit());
    }

    @Override
    protected LdbcQuery10Result toResult(Map<String, Object> record) {
        long personId = getLong(record, "personId");
        String personFirstName = getString(record, FIRST_NAME);
        String personLastName = getString(record, LAST_NAME);
        int commonInterestScore = getInt(record, "commonInterestScore");
        String personGender = getString(record, GENDER);
        String personCityName = getString(record, "cityName");

        return new LdbcQuery10Result(
                personId,
                personFirstName,
                personLastName,
                commonInterestScore,
                personGender,
                personCityName
        );
    }
}
