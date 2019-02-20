/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries;

import com.neo4j.bench.client.ClientUtil;

import java.util.Optional;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;

import static com.neo4j.bench.client.StoreClient.VERSION;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class VerifyStoreSchema implements Query<Void>
{
    public VerifyStoreSchema()
    {
    }

    @Override
    public Void execute( Driver driver )
    {
        System.out.println( "Verifying store schema..." );
        long start = System.currentTimeMillis();
        verifyStoreSchema( driver );
        long duration = System.currentTimeMillis() - start;
        System.out.println( format( "Verified in %s", ClientUtil.durationToString( duration ) ) );
        return null;
    }

    @Override
    public Optional<String> nonFatalError()
    {
        return Optional.empty();
    }

    private void verifyStoreSchema( Driver driver )
    {
        try ( Session session = driver.session() )
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

            assertManyToOne( "TestRun nodes connect to Environment nodes correctly",
                             "(:TestRun)", "-[:IN_ENVIRONMENT]->", "(:Environment)", session );

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

            assertThat( "Metrics & TestRun nodes connect to Neo4jConfig nodes correctly",
                        patternCountInStore( "(:Neo4jConfig)", session ),
                        equalTo(
                                patternCountInStore( "(:Metrics)-[:HAS_CONFIG]->()", session ) +
                                patternCountInStore( "(:TestRun)-[:HAS_CONFIG]->()", session ) ) );

            assertManyToOne( "Benchmark nodes connect to BenchmarkGroup nodes correctly",
                             "(:Benchmark)", "<-[:HAS_BENCHMARK]-", "(:BenchmarkGroup)", session );

            assertOneToOne( "Benchmark nodes connect to BenchmarkParam nodes correctly",
                            "(:Benchmark)", "-[:HAS_PARAMS]->", "(:BenchmarkParams)", session );

            assertManyToOne( "Plan nodes connect to Metrics nodes correctly",
                             "(:Plan)", "<-[:HAS_PLAN]-", "(:Metrics)", session );

            assertManyToOne( "Plan nodes connect to PlanTree nodes correctly",
                             "(:Plan)", "-[:HAS_PLAN_TREE]->", "(:PlanTree)", session );

            assertOneToOne( "Plan nodes connect to CompilationMetrics nodes correctly",
                            "(:Plan)", "-[:HAS_COMPILATION_METRICS]->", "(:CompilationMetrics)", session );

            assertManyToOne( "PlanTree nodes connect to Operator nodes correctly",
                             "(:PlanTree)", "-[:HAS_OPERATORS]->", "(:Operator)", session );

            assertManyToMany( "BenchmarkGroup nodes connect to BenchmarkTool correctly",
                              "(:BenchmarkTool)", "-[:IMPLEMENTS]->", "(:BenchmarkGroup)", session );

            assertThat( "Metrics & TestRun nodes connect to Annotation nodes correctly",
                        patternCountInStore( "(:Annotation)", session ),
                        equalTo(
                                patternCountInStore( "(:Metrics)-[:WITH_ANNOTATION]->()", session ) +
                                patternCountInStore( "(:TestRun)-[:WITH_ANNOTATION]->()", session ) ) );

            // --------------------------------------------------------------------------------------------------------
            // ------------------------------- Isolated Nodes ---------------------------------------------------------
            // --------------------------------------------------------------------------------------------------------

            assertThat( "All BenchmarkTool should have at least one relationship",
                        isolatedCountForLabel( "BenchmarkTool", session ), equalTo( 0 ) );

            assertThat( "All BenchmarkToolVersion should have at least one relationship",
                        isolatedCountForLabel( "BenchmarkToolVersion", session ), equalTo( 0 ) );

            assertThat( "All Project should have at least one relationship",
                        isolatedCountForLabel( "Project", session ), equalTo( 0 ) );

            assertThat( "All Neo4jConfig should have exactly one relationship",
                        isolatedCountForLabel( "Neo4jConfig", session ), equalTo( 0 ) );

            assertThat( "All BenchmarkConfig should have exactly one relationship",
                        isolatedCountForLabel( "BenchmarkConfig", session ), equalTo( 0 ) );

            assertThat( "All Java should have at least one relationship",
                        isolatedCountForLabel( "Java", session ), equalTo( 0 ) );

            assertThat( "All Environment should have at least one relationship",
                        isolatedCountForLabel( "Environment", session ), equalTo( 0 ) );

            assertThat( "All BenchmarkParams should have exactly one relationship",
                        isolatedCountForLabel( "BenchmarkParams", session ), equalTo( 0 ) );

            assertThat( "All BenchmarkGroup should have at least one relationship",
                        isolatedCountForLabel( "BenchmarkGroup", session ), equalTo( 0 ) );

            assertThat( "All Benchmark should have at least one relationship",
                        isolatedCountForLabel( "Benchmark", session ), equalTo( 0 ) );

            assertThat( "All Profiles should have at exactly one relationship",
                        isolatedCountForLabel( "Profiles", session ), equalTo( 0 ) );

            assertThat( "All Annotation should have at exactly one relationship",
                        isolatedCountForLabel( "Annotation", session ), equalTo( 0 ) );

            // --------------------------------------------------------------------------------------------------------
            // ------------------------------- No Unexpected Nodes ----------------------------------------------------
            // --------------------------------------------------------------------------------------------------------

            assertThat( "All Neo4j nodes have been removed",
                        patternCountInStore( "(:Neo4j)", session ), equalTo( 0 ) );
            assertThat( "All WITH_NEO4J relationships have been removed",
                        patternCountInStore( "()-[:WITH_NEO4J]->()", session ), equalTo( 0 ) );
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
        assertThat( reason + "\n" + countsString, countLeftRel, equalTo( countRightRel ) );
        assertThat( reason + "\n" + countsString, countRel, equalTo( countLeftRel ) );
        assertThat( reason + "\n" + countsString, countRel, equalTo( countRightRel ) );
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
        assertThat( reason,
                    patternCountInStore( leftNode, session ),
                    equalTo( patternCountInStore( rightNode, session ) ) );
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
        assertThat( reason + "\n" + countsString, countLeft, equalTo( countLeftRel ) );
        assertThat( reason + "\n" + countsString, countLeft, equalTo( countRightRel ) );
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
}
