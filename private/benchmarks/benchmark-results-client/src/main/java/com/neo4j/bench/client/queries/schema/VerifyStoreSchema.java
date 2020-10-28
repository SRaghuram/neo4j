/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries.schema;

import com.neo4j.bench.client.queries.Query;
import com.neo4j.bench.common.util.BenchmarkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import static com.neo4j.bench.client.StoreClient.VERSION;
import static java.lang.String.format;
import static org.neo4j.driver.AccessMode.READ;

public class VerifyStoreSchema implements Query<Void>
{

    private static final Logger LOG = LoggerFactory.getLogger( VerifyStoreSchema.class );

    @Override
    public Void execute( Driver driver )
    {
        LOG.debug( "Verifying store schema..." );
        Instant start = Instant.now();
        verifyStoreSchema( driver );
        Duration duration = Duration.between( start, Instant.now() );
        LOG.debug( format( "Verified in %s", BenchmarkUtil.durationToString( duration ) ) );
        return null;
    }

    private void verifyStoreSchema( Driver driver )
    {
        try ( Session session = driver.session( SessionConfig.builder().withDefaultAccessMode( READ ).build() ) )
        {
            assertExactlyOneStoreVersion( session );
            assertCorrectStoreVersion( session );

            // --------------------------------------------------------------------------------------------------------
            // ------------------------------- Schema Structure -------------------------------------------------------
            // --------------------------------------------------------------------------------------------------------

            assertOneToOne( "TestRun nodes connect to BenchmarkConfig nodes correctly",
                            "(:TestRun)", "-[:HAS_BENCHMARK_CONFIG]->", "(:BenchmarkConfig)", session );

            assertManyToOne( "TestRun nodes connect to Java nodes correctly",
                             "(:TestRun)", "-[:WITH_JAVA]->", "(:Java)", session );

            assertOneToOne( "TestRun nodes connect to Environment nodes correctly",
                            "(:TestRun)", "-[:IN_ENVIRONMENT]->", "(:Environment)", session );

            assertManyToMany( "Environment nodes connect to Instance nodes correctly",
                             "(:Environment)", "-[:HAS_INSTANCE]->", "(:Instance)", session );

            assertManyToOne( "TestRun nodes connect to BenchmarkToolVersion nodes correctly",
                             "(:TestRun)", "-[:WITH_TOOL]->", "(:BenchmarkToolVersion)", session );

            assertManyToOne( "BenchmarkToolVersion nodes connect to BenchmarkTool nodes correctly",
                             "(:BenchmarkToolVersion)", "-[:VERSION_OF]->", "(:BenchmarkTool)", session );

            assertManyToMany( "TestRun nodes connect to Project nodes correctly",
                              "(:Project)", "<-[:WITH_PROJECT]-", "(:TestRun)", session );

            assertManyToOne( "Metrics nodes connect to TestRun nodes correctly",
                             "(:Metrics)", "<-[:HAS_METRICS]-", "(:TestRun)", session );

            assertManyToOne( "Profiles nodes connect to Metrics nodes correctly",
                             "(:Profiles)", "<-[:HAS_PROFILES]-", "(:Metrics)", session );

            assertManyToOne( "Metrics nodes connect to Benchmark nodes correctly",
                             "(:Metrics)", "-[:METRICS_FOR]->", "(:Benchmark)", session );

            assertEqual( "Metrics & TestRun nodes connect to Neo4jConfig nodes correctly",
                         patternCountInStore( "(:Neo4jConfig)", session ),
                         patternCountInStore( "(:Metrics)-[:HAS_CONFIG]->()", session ) +
                         patternCountInStore( "(:TestRun)-[:HAS_CONFIG]->()", session ) );

            assertManyToOne( "Benchmark nodes connect to BenchmarkGroup nodes correctly",
                             "(:Benchmark)", "<-[:HAS_BENCHMARK]-", "(:BenchmarkGroup)", session );

            assertOneToOne( "Benchmark nodes connect to BenchmarkParam nodes correctly",
                            "(:Benchmark)", "-[:HAS_PARAMS]->", "(:BenchmarkParams)", session );

            assertManyToOne( "Plan nodes connect to Metrics nodes correctly",
                             "(:Plan)", "<-[:HAS_PLAN]-", "(:Metrics)", session );

            assertManyToOne( "Plan nodes connect to PlanTree nodes correctly",
                             "(:Plan)", "-[:HAS_PLAN_TREE]->", "(:PlanTree)", session );

            assertManyToOne( "PlanTree nodes connect to Operator nodes correctly",
                             "(:PlanTree)", "-[:HAS_OPERATORS]->", "(:Operator)", session );

            assertManyToOne( "BenchmarkGroup nodes connect to BenchmarkTool correctly",
                             "(:BenchmarkGroup)", "<-[:IMPLEMENTS]-", "(:BenchmarkTool)", session );

            assertEqual( "Metrics & TestRun nodes connect to Annotation nodes correctly",
                         patternCountInStore( "(:Annotation)", session ),
                         patternCountInStore( "(:Metrics)-[:WITH_ANNOTATION]->()", session ) +
                         patternCountInStore( "(:TestRun)-[:WITH_ANNOTATION]->()", session ) );

            assertOneToOneWhenLhsExists( "AuxiliaryMetrics nodes connect to Metrics nodes correctly",
                                         "(:Metrics)", "-[:HAS_AUXILIARY_METRICS]->", "(:AuxiliaryMetrics)", session );

            assertManyToOne( "Error nodes connect to TestRun correctly",
                             "(:Error)", "<-[:HAS_ERROR]-", "(:TestRun)", session );
            assertManyToOne( "Error nodes connect to Benchmark correctly",
                              "(:Error)", "-[:ERROR_FOR]->", "(:Benchmark)", session );

            // --------------------------------------------------------------------------------------------------------
            // ------------------------------- Isolated Nodes ---------------------------------------------------------
            // --------------------------------------------------------------------------------------------------------

            assertEqual( "All BenchmarkTool should have at least one relationship",
                         isolatedCountForLabel( "BenchmarkTool", session ), 0 );

            assertEqual( "All BenchmarkToolVersion should have at least one relationship",
                         isolatedCountForLabel( "BenchmarkToolVersion", session ), 0 );

            assertEqual( "All Project should have at least one relationship",
                         isolatedCountForLabel( "Project", session ), 0 );

            assertEqual( "All Neo4jConfig should have at least one relationship",
                         isolatedCountForLabel( "Neo4jConfig", session ), 0 );

            assertEqual( "All BenchmarkConfig should have at least one relationship",
                         isolatedCountForLabel( "BenchmarkConfig", session ), 0 );

            assertEqual( "All Java should have at least one relationship",
                         isolatedCountForLabel( "Java", session ), 0 );

            assertEqual( "All Environment should have at least one relationship",
                         isolatedCountForLabel( "Environment", session ), 0 );

            assertEqual( "All BenchmarkParams should have at least one relationship",
                         isolatedCountForLabel( "BenchmarkParams", session ), 0 );

            assertEqual( "All BenchmarkGroup should have at least one relationship",
                         isolatedCountForLabel( "BenchmarkGroup", session ), 0 );

            assertEqual( "All Benchmark should have at least one relationship",
                         isolatedCountForLabel( "Benchmark", session ), 0 );

            assertEqual( "All Profiles should have at least one relationship",
                         isolatedCountForLabel( "Profiles", session ), 0 );

            assertEqual( "All Annotation should have at least one relationship",
                         isolatedCountForLabel( "Annotation", session ), 0 );

            assertEqual( "All AuxiliaryMetrics should have at least one relationship",
                         isolatedCountForLabel( "AuxiliaryMetrics", session ), 0 );

            // --------------------------------------------------------------------------------------------------------
            // ------------------------------- No Unexpected Nodes ----------------------------------------------------
            // --------------------------------------------------------------------------------------------------------

            assertEqual( "All Neo4j nodes have been removed",
                         patternCountInStore( "(:Neo4j)", session ), 0 );
            assertEqual( "All WITH_NEO4J relationships have been removed",
                         patternCountInStore( "()-[:WITH_NEO4J]->()", session ), 0 );
        }
    }

    private void assertManyToMany(
            String reason,
            String leftNode,
            String relExpression,
            String rightNode,
            Session session )
    {
        String leftRelPattern = leftNode + relExpression + "()";
        int countLeftRel = patternCountInStore( leftRelPattern, session );
        String rightRelPattern = "()" + relExpression + rightNode;
        int countRightRel = patternCountInStore( rightRelPattern, session );
        String relPattern = "()" + relExpression + "()";
        int countRel = patternCountInStore( relPattern, session );
        String countsString = "   *  " + relPattern + " = " + countRel + "\n" +
                              "   * " + leftRelPattern + " = " + countLeftRel + "\n" +
                              "   * " + rightRelPattern + " = " + countRightRel;
        assertEqual( reason + "\n" + countsString, countLeftRel, countRightRel );
        assertEqual( reason + "\n" + countsString, countRel, countLeftRel );
        assertEqual( reason + "\n" + countsString, countRel, countRightRel );
    }

    /**
     * Some nodes of the schema are optional, but when they do exist they should connect to exactly one other node, with exactly one relationship. This method
     * performs this check.
     *
     * @param leftNode              node that is always present, i.e., not optional
     * @param optionalRelExpression relationship that connects LHS to RHS nodes, when the optional RHS node is present
     * @param optionalRightNode     optional node, i.e., not always present
     */
    private void assertOneToOneWhenLhsExists(
            String reason,
            String leftNode,
            String optionalRelExpression,
            String optionalRightNode,
            Session session )
    {
        int maybeRightCount = patternCountInStore( optionalRightNode, session );
        int maybeRelCount = patternCountInStore( "()" + optionalRelExpression + "()", session );
        int maybeRightAndMaybeRelCount = patternCountInStore( "()" + optionalRelExpression + optionalRightNode, session );
        int leftAndMaybeRelCount = patternCountInStore( leftNode + optionalRelExpression + "()", session );
        assertEqual( reason + "\nEvery RHS node should have exactly one relationship", maybeRightCount, maybeRightAndMaybeRelCount );
        assertEqual( reason + "\nEvery RHS node should have exactly one relationship", maybeRelCount, maybeRightAndMaybeRelCount );
        assertEqual( reason + "\nEvery RHS node should connect to expected LHS node", leftAndMaybeRelCount, maybeRightAndMaybeRelCount );
    }

    // "approximate" because asserts only work on totals/averages, not on every entity
    // (1) left node count = left out degree --> average left degree = 1 --> approximates left rel existence constraint
    // (2) average left degree = average right degree --> every left relationship connects to a right node
    //                                                --> approximates 1:1 cardinality constraint
    // (3) left node count = right node count --> further approximates 1:1 cardinality constraint
    private void assertOneToOne(
            String reason,
            String leftNode,
            String relExpression,
            String rightNode,
            Session session )
    {
        assertManyToOne( reason, leftNode, relExpression, rightNode, session );
        assertEqual( reason,
                     patternCountInStore( leftNode, session ),
                     patternCountInStore( rightNode, session ) );
    }

    // "approximate" because asserts only work on totals/averages, not on every entity
    // (1) left node count = left out degree --> average left degree = 1 --> approximates left rel existence constraint
    // (2) average left degree = average right degree --> every left relationship connects to a right node
    //                                                --> also approximates 1:1 cardinality constraint
    private void assertManyToOne(
            String reason,
            String leftNode,
            String relExpression,
            String rightNode,
            Session session )
    {
        int countLeft = patternCountInStore( leftNode, session );
        String leftRelPattern = leftNode + relExpression + "()";
        int countLeftRel = patternCountInStore( leftRelPattern, session );
        String rightRelPattern = "()" + relExpression + rightNode;
        int countRightRel = patternCountInStore( rightRelPattern, session );
        String countsString = "   * " + leftNode + " = " + countLeft + "\n" +
                              "   * " + leftRelPattern + " = " + countLeftRel + "\n" +
                              "   * " + rightRelPattern + " = " + countRightRel;
        assertEqual( reason + "\n" + countsString, countLeft, countLeftRel );
        assertEqual( reason + "\n" + countsString, countLeft, countRightRel );
    }

    private int isolatedCountForLabel( String labelName, Session session )
    {
        return session.run( format( "MATCH (n:%s) " +
                                    "WHERE size((n)--())=0 " +
                                    "RETURN count(*)", labelName ) ).single().get( "count(*)" ).asInt();
    }

    public static int patternCountInStore( String pattern, Session session )
    {
        return session.run( format( "MATCH %s RETURN count(*)", pattern ) ).single().get( "count(*)" ).asInt();
    }

    private void assertExactlyOneStoreVersion( Session session )
    {
        Record result = session.run( "MATCH (ss:StoreSchema) RETURN count(ss) AS c" ).single();
        long count = result.get( "c" ).asLong();
        if ( count != 1 )
        {
            throw new RuntimeException( format( "Expected 1 store version node, but found %s", count ) );
        }
    }

    private void assertCorrectStoreVersion( Session session )
    {
        Record result = session.run( "MATCH (ss:StoreSchema) RETURN ss.version AS v" ).single();
        long version = result.get( "v" ).asLong();
        if ( version != VERSION )
        {
            throw new RuntimeException( format( "Expected store version %s but found %s", VERSION, version ) );
        }
    }

    private void assertEqual( String errorMessage, long a, long b )
    {
        if ( a != b )
        {
            throw new AssertionError( format( "%s <> %s\n%s", a, b, errorMessage ) );
        }
    }
}
