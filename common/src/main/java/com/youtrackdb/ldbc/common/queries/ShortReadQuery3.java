package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery3PersonFriends;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery3PersonFriendsResult;

import java.util.Map;

import static com.youtrackdb.ldbc.common.GremlinHelpers.*;
import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;

/**
 * IS3: Friends of a person
 *
 * Given a start Person, retrieve all of their friends, and the date at which they became friends.
 */
public class ShortReadQuery3 extends ListQueryHandler<LdbcShortQuery3PersonFriends, LdbcShortQuery3PersonFriendsResult> {

    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcShortQuery3PersonFriends operation, GraphTraversalSource g) {

        return g.V()
                .has(PERSON, ID, operation.getPersonIdSQ3())
                .outE(KNOWS).as("knows")
                .inV()
                .order()
                    .by(select("knows").values(CREATION_DATE), Order.desc)
                    .by(ID, Order.asc)
                .project("personId", FIRST_NAME, LAST_NAME, "friendshipCreationDate")
                    .by(ID)
                    .by(FIRST_NAME)
                    .by(LAST_NAME)
                    .by(select("knows").values(CREATION_DATE));
    }

    @Override
    protected LdbcShortQuery3PersonFriendsResult toResult(Map<String, Object> record) {
        long personId = getLong(record, "personId");
        String personFirstName = getString(record, FIRST_NAME);
        String personLastName = getString(record, LAST_NAME);
        long friendshipCreationDate = getDateAsMillis(record, "friendshipCreationDate");

        return new LdbcShortQuery3PersonFriendsResult(
                personId,
                personFirstName,
                personLastName,
                friendshipCreationDate
        );
    }
}
