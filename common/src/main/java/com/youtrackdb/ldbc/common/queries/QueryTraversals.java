package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.*;

import java.util.Collections;
import java.util.Date;
import java.util.Map;

/**
 * Public facade exposing traversal building for LDBC SNB Interactive Complex Read queries.
 * Same-package access to the {@code protected buildTraversal()} methods.
 */
public final class QueryTraversals {

    private QueryTraversals() {}

    public static GraphTraversal<?, Map<String, Object>> ic1(GraphTraversalSource g, long personId, String firstName, int limit) {
        return new ComplexReadQuery1().buildTraversal(new LdbcQuery1(personId, firstName, limit), g);
    }

    public static GraphTraversal<?, Map<String, Object>> ic2(GraphTraversalSource g, long personId, Date maxDate, int limit) {
        return new ComplexReadQuery2().buildTraversal(new LdbcQuery2(personId, maxDate, limit), g);
    }

    public static GraphTraversal<?, Map<String, Object>> ic3(GraphTraversalSource g, long personId, String countryX, String countryY, Date startDate, int durationDays, int limit) {
        return new ComplexReadQuery3().buildTraversal(new LdbcQuery3(personId, countryX, countryY, startDate, durationDays, limit), g);
    }

    public static GraphTraversal<?, Map<String, Object>> ic4(GraphTraversalSource g, long personId, Date startDate, int durationDays, int limit) {
        return new ComplexReadQuery4().buildTraversal(new LdbcQuery4(personId, startDate, durationDays, limit), g);
    }

    public static GraphTraversal<?, Map<String, Object>> ic5(GraphTraversalSource g, long personId, Date minDate, int limit) {
        return new ComplexReadQuery5().buildTraversal(new LdbcQuery5(personId, minDate, limit), g);
    }

    public static GraphTraversal<?, Map<String, Object>> ic6(GraphTraversalSource g, long personId, String tagName, int limit) {
        return new ComplexReadQuery6().buildTraversal(new LdbcQuery6(personId, tagName, limit), g);
    }

    public static GraphTraversal<?, Map<String, Object>> ic7(GraphTraversalSource g, long personId, int limit) {
        return new ComplexReadQuery7().buildTraversal(new LdbcQuery7(personId, limit), g);
    }

    public static GraphTraversal<?, Map<String, Object>> ic8(GraphTraversalSource g, long personId, int limit) {
        return new ComplexReadQuery8().buildTraversal(new LdbcQuery8(personId, limit), g);
    }

    public static GraphTraversal<?, Map<String, Object>> ic9(GraphTraversalSource g, long personId, Date maxDate, int limit) {
        return new ComplexReadQuery9().buildTraversal(new LdbcQuery9(personId, maxDate, limit), g);
    }

    public static GraphTraversal<?, Map<String, Object>> ic10(GraphTraversalSource g, long personId, int month, int limit) {
        return new ComplexReadQuery10().buildTraversal(new LdbcQuery10(personId, month, limit), g);
    }

    public static GraphTraversal<?, Map<String, Object>> ic11(GraphTraversalSource g, long personId, String countryName, int workFromYear, int limit) {
        return new ComplexReadQuery11().buildTraversal(new LdbcQuery11(personId, countryName, workFromYear, limit), g);
    }

    public static GraphTraversal<?, Map<String, Object>> ic12(GraphTraversalSource g, long personId, String tagClassName, int limit) {
        return new ComplexReadQuery12().buildTraversal(new LdbcQuery12(personId, tagClassName, limit), g);
    }

    public static GraphTraversal<?, Map<String, Object>> ic13(GraphTraversalSource g, long person1Id, long person2Id) {
        return new ComplexReadQuery13().buildTraversal(new LdbcQuery13(person1Id, person2Id), g, Collections.emptyMap());
    }

    public static GraphTraversal<?, Map<String, Object>> ic14(GraphTraversalSource g, long person1Id, long person2Id) {
        return new ComplexReadQuery14().buildTraversal(new LdbcQuery14(person1Id, person2Id), g, Collections.emptyMap());
    }
}
