/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries;

import com.neo4j.bench.client.model.Annotation;
import com.neo4j.bench.client.util.Resources;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import static com.neo4j.bench.client.ClientUtil.prettyPrint;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AttachMetricsAnnotation implements Query<Void>, EmbeddedQuery<Node>
{
    private static final String ATTACH_ANNOTATION = Resources.fileToString( "/queries/write/attach_metrics_annotation.cypher" );

    private String testRunId;
    private Annotation annotation;
    private String benchmarkName;
    private String benchmarkGroupName;

    public AttachMetricsAnnotation(
            String testRunId,
            String benchmarkName,
            String benchmarkGroupName,
            Annotation annotation )
    {
        this.testRunId = requireNonNull( testRunId );
        this.annotation = requireNonNull( annotation );
        this.benchmarkName = requireNonNull( benchmarkName );
        this.benchmarkGroupName = requireNonNull( benchmarkGroupName );
    }

    @Override
    public Void execute( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            StatementResult statementResult = session.run( ATTACH_ANNOTATION, params() );
            int annotationsCreated = statementResult.consume().counters().nodesCreated();
            if ( 1 != annotationsCreated )
            {
                throw new RuntimeException(
                        format( "Expected to create 1 annotation but created %s\n" +
                                "Failed to create annotation %s for:\n" +
                                " * Test run '%s'\n" +
                                " * Benchmark '%s'\n" +
                                " * Benchmark group '%s'",
                                annotationsCreated,
                                annotation,
                                testRunId,
                                benchmarkName,
                                benchmarkGroupName ) );
            }
        }
        return null;
    }

    @Override
    public Node execute( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Result result = db.execute( ATTACH_ANNOTATION, params() );
            int annotationsCreated = result.getQueryStatistics().getNodesCreated();
            if ( 1 != annotationsCreated )
            {
                throw new RuntimeException(
                        format( "Expected to create 1 annotation but created %s\n" +
                                "Failed to create annotation %s for:\n" +
                                " * Test run '%s'\n" +
                                " * Benchmark '%s'\n" +
                                " * Benchmark group '%s'",
                                annotationsCreated,
                                annotation,
                                testRunId,
                                benchmarkName,
                                benchmarkGroupName ) );
            }
            tx.success();
            return (Node) result.next().get( "a" );
        }
    }

    private Map<String,Object> params()
    {
        Map<String,Object> params = new HashMap<>();
        params.put( "test_run_id", testRunId );
        params.put( "benchmark_name", benchmarkName );
        params.put( "benchmark_group_name", benchmarkGroupName );
        params.put( "annotation", annotation.toMap() );
        return params;
    }

    @Override
    public String toString()
    {
        return "Params:\n" + prettyPrint( params(), "\t" ) + ATTACH_ANNOTATION;
    }

    @Override
    public Optional<String> nonFatalError()
    {
        return Optional.empty();
    }
}
