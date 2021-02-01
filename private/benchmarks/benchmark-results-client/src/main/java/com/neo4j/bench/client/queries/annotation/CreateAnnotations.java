/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries.annotation;

import com.neo4j.bench.client.queries.Query;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.model.Repository;
import com.neo4j.bench.model.util.MapPrinter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.summary.ResultSummary;

import static com.neo4j.bench.client.queries.annotation.CreateAnnotations.AnnotationTarget.METRICS;
import static com.neo4j.bench.client.queries.annotation.CreateAnnotations.AnnotationTarget.TEST_RUN;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class CreateAnnotations implements Query<CreateAnnotationsResult>
{
    private static final String ANNOTATE_TEST_RUNS = Resources.fileToString( "/queries/annotations/annotate_test_runs.cypher" );
    private static final String ANNOTATE_METRICS = Resources.fileToString( "/queries/annotations/annotate_metrics.cypher" );

    public enum AnnotationTarget
    {
        TEST_RUN,
        METRICS
    }

    private final long packagingBuildId;
    private final String comment;
    private final String author;
    private final String neo4jSeries;
    private final List<String> benchmarkTools;
    private final Set<AnnotationTarget> annotationTargets;

    public CreateAnnotations( long packagingBuildId,
                              String comment,
                              String author,
                              String neo4jSeries,
                              List<Repository> benchmarkTools,
                              Set<AnnotationTarget> annotationTargets )
    {
        this.packagingBuildId = packagingBuildId;
        this.comment = comment;
        this.author = author;
        this.neo4jSeries = neo4jSeries;
        if ( annotationTargets.isEmpty() )
        {
            throw new IllegalStateException( format( "Must annotate at least one of: %s", Arrays.toString( AnnotationTarget.values() ) ) );
        }
        this.annotationTargets = annotationTargets;
        this.benchmarkTools = benchmarkTools.stream()
                                            .peek( r ->
                                                   {
                                                       if ( !isSupportedTool( r ) )
                                                       {
                                                           throw new IllegalArgumentException( "Not a valid/supported benchmark tool: " + r );
                                                       }
                                                   } )
                                            .map( Repository::projectName )
                                            .collect( toList() );
    }

    @Override
    public CreateAnnotationsResult execute( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            long createdTestRunAnnotations = 0;
            long createdMetricsAnnotations = 0;
            if ( annotationTargets.contains( TEST_RUN ) )
            {
                ResultSummary result = session.run( ANNOTATE_TEST_RUNS, params() ).consume();
                createdTestRunAnnotations = result.counters().nodesCreated();
            }
            if ( annotationTargets.contains( METRICS ) )
            {
                ResultSummary result = session.run( ANNOTATE_METRICS, params() ).consume();
                createdMetricsAnnotations = result.counters().nodesCreated();
            }
            return new CreateAnnotationsResult( createdTestRunAnnotations, createdMetricsAnnotations );
        }
    }

    public static boolean isSupportedTool( Repository repository )
    {
        switch ( repository )
        {
        case CAPS:
        case ALGOS:
        case MORPHEUS_BENCH:
        case NEO4J:
            return false;
        default:
            return true;
        }
    }

    private Map<String,Object> params()
    {
        Map<String,Object> params = new HashMap<>();
        params.put( "packaging_build_id", packagingBuildId );
        params.put( "comment", comment );
        params.put( "author", author );
        params.put( "neo4j_branch", neo4jSeries );
        params.put( "benchmark_tools", benchmarkTools );
        return params;
    }

    @Override
    public String toString()
    {
        return "Params:\n" + MapPrinter.prettyPrint( params() ) + ANNOTATE_TEST_RUNS;
    }
}
