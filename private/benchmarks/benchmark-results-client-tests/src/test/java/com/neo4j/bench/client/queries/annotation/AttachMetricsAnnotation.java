/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries.annotation;

import com.neo4j.bench.client.queries.Query;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.model.Annotation;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.TestRun;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import static com.neo4j.bench.model.util.MapPrinter.prettyPrint;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AttachMetricsAnnotation implements Query<Void>
{
    private static final String ATTACH_ANNOTATION = Resources.fileToString( "/queries/annotations/attach_metrics_annotation.cypher" );

    private final TestRun testRun;
    private final Annotation annotation;
    private final Benchmark benchmark;
    private final BenchmarkGroup benchmarkGroup;

    public AttachMetricsAnnotation(
            TestRun testRun,
            Benchmark benchmark,
            BenchmarkGroup benchmarkGroup,
            Annotation annotation )
    {
        this.testRun = requireNonNull( testRun );
        this.annotation = requireNonNull( annotation );
        this.benchmark = requireNonNull( benchmark );
        this.benchmarkGroup = requireNonNull( benchmarkGroup );
    }

    @Override
    public Void execute( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            Result statementResult = session.run( ATTACH_ANNOTATION, params() );
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
                                testRun.id(),
                                benchmark.name(),
                                benchmarkGroup.name() ) );
            }
        }
        return null;
    }

    private Map<String,Object> params()
    {
        Map<String,Object> params = new HashMap<>();
        params.put( "test_run_id", testRun.id() );
        params.put( "benchmark_name", benchmark.name() );
        params.put( "benchmark_group_name", benchmarkGroup.name() );
        params.put( "annotation", annotation.toMap() );
        return params;
    }

    @Override
    public String toString()
    {
        return "Params:\n" + prettyPrint( params() ) + ATTACH_ANNOTATION;
    }

    @Override
    public Optional<String> nonFatalError()
    {
        return Optional.empty();
    }
}
