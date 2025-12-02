package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery14;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery14Result;

import java.util.*;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.Operator.sum;
import static org.apache.tinkerpop.gremlin.process.traversal.P.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

/**
 * IC14: Trusted connection paths (v1)
 *
 * Given two Persons, find all (unweighted) shortest paths between these two Persons in the subgraph
 * induced by the knows relationship. Then, for each path calculate a weight. The weight of a path is
 * the sum of weights between every pair of consecutive Person nodes in the path. The weight for a
 * pair of Persons is calculated based on their interactions:
 * - Every direct reply to a Post is 1.0
 * - Every direct reply to a Comment is 0.5
 * Interactions are counted both ways. Return all the paths with shortest length and their weights.
 * Return all the paths with shortest length and their weights. Do not return any rows if there is no
 * path between the two Persons.
 */
public class ComplexReadQuery14 extends ListQueryHandler<LdbcQuery14, LdbcQuery14Result> {

    private static final int DEFAULT_MAX_HOPS = 4;

    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcQuery14 operation, GraphTraversalSource g, Map<String, String> properties) {
        // IC14's Interactive-v2 parameters are curated to be either unreachable or max 4 hops apart.
        // We rely on the same guarantee here (Interactive v1)
        // to keep the traversal from exploring arbitrarily deep and blowing up work.
        int maxHops = Integer.parseInt(properties.getOrDefault("tinkerpop.ic14.maxHops", String.valueOf(DEFAULT_MAX_HOPS)));
        return buildTraversal(operation, g, maxHops);
    }

    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcQuery14 operation, GraphTraversalSource g) {
        return buildTraversal(operation, g, DEFAULT_MAX_HOPS);
    }

    private GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcQuery14 operation, GraphTraversalSource g, int maxHops) {
        long person1Id = operation.getPerson1IdQ14StartNode();
        long person2Id = operation.getPerson2IdQ14EndNode();

        // Find all shortest paths and calculate weights
        return g.withSack(0)
                .V()
                .has(PERSON, ID, person1Id)
                .repeat(outE(KNOWS).as("edge").inV().hasLabel(PERSON).simplePath().sack(sum).by(constant(1)))
                .until(has(ID, eq(person2Id)).or().sack().is(maxHops))
                .has(ID, eq(person2Id))
                .project("items", "edges")
                    .by(sack())
                    .by(select(Pop.all, "edge"))
                .group()
                    .by("items")
                    .by("edges")
                .unfold()
                .order()
                    .by(Column.keys)
                .limit(1)
                .select(Column.values)
                .unfold()
                .project("personIdsInPath", "pathWeight")
                .by(unfold()
                        .project("from", "to")
                        .by(outV().values(ID))
                        .by(inV().values(ID))
                        .unfold()
                        .select(Column.values)
                        .dedup()
                        .fold())
                .by(unfold().map(as("edge").outV().as("a").select("edge").inV().as("b").
                        project("postAB", "commentAB", "postBA", "commentBA")
                        .by(select("a").in(HAS_CREATOR).hasLabel(COMMENT)
                                .where(out(REPLY_OF).hasLabel(POST)
                                        .out(HAS_CREATOR).where(eq("b")))
                                .count())
                        .by(select("a").in(HAS_CREATOR).hasLabel(COMMENT)
                                .where(out(REPLY_OF).hasLabel(COMMENT)
                                        .out(HAS_CREATOR).where(eq("b")))
                                .count())
                        .by(select("b").in(HAS_CREATOR).hasLabel(COMMENT)
                                .where(out(REPLY_OF).hasLabel(POST)
                                        .out(HAS_CREATOR).where(eq("a")))
                                .count())
                        .by(select("b").in(HAS_CREATOR).hasLabel(COMMENT)
                                .where(out(REPLY_OF).hasLabel(COMMENT)
                                        .out(HAS_CREATOR).where(eq("a")))
                                .count())
                        .math("postAB + 0.5 * commentAB + postBA + 0.5 * commentBA")
                ).sum())
                .order()
                    .by("pathWeight", Order.desc);
    }

    @Override
    protected LdbcQuery14Result toResult(Map<String, Object> record) {
        @SuppressWarnings("unchecked")
        List<Long> personIds = (List<Long>) record.get("personIdsInPath");
        Object weightObj = record.get("pathWeight");
        double weight = weightObj instanceof Number ? ((Number) weightObj).doubleValue() : 0.0;

        return new LdbcQuery14Result(personIds, weight);
    }
}
