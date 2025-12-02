package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery5MessageCreator;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery5MessageCreatorResult;

import java.util.Map;

import static com.youtrackdb.ldbc.common.GremlinHelpers.*;
import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;

/**
 * IS5: Creator of a message
 *
 * Given a Message, retrieve its author.
 */
public class ShortReadQuery5 extends SingleResultQueryHandler<LdbcShortQuery5MessageCreator, LdbcShortQuery5MessageCreatorResult> {

    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcShortQuery5MessageCreator operation, GraphTraversalSource g) {

        return g.V()
                .hasLabel(POST, COMMENT)
                .has(ID, operation.getMessageIdCreator())
                .out(HAS_CREATOR)
                .project("personId", FIRST_NAME, LAST_NAME)
                    .by(ID)
                    .by(FIRST_NAME)
                    .by(LAST_NAME);
    }

    @Override
    protected LdbcShortQuery5MessageCreatorResult toResult(Map<String, Object> record) {
        long personId = getLong(record, "personId");
        String personFirstName = getString(record, FIRST_NAME);
        String personLastName = getString(record, LAST_NAME);

        return new LdbcShortQuery5MessageCreatorResult(
                personId,
                personFirstName,
                personLastName
        );
    }
}
