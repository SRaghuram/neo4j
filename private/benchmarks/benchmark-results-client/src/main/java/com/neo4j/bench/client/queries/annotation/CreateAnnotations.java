/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries.annotation;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.queries.Query;
import com.neo4j.bench.common.model.Repository;
import com.neo4j.bench.common.util.Resources;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;

import static com.neo4j.bench.client.queries.annotation.CreateAnnotations.AnnotationTarget.BOTH_TEST_RUN_AND_METRICS;
import static com.neo4j.bench.client.queries.annotation.CreateAnnotations.AnnotationTarget.ONLY_METRICS;
import static com.neo4j.bench.client.queries.annotation.CreateAnnotations.AnnotationTarget.ONLY_TEST_RUN;
import static com.neo4j.bench.common.util.BenchmarkUtil.prettyPrint;
import static java.util.stream.Collectors.toList;

public class CreateAnnotations implements Query<Void>
{
    private static final String ANNOTATE_TEST_RUNS = Resources.fileToString( "/queries/annotations/annotate_test_runs.cypher" );
    private static final String ANNOTATE_METRICS = Resources.fileToString( "/queries/annotations/annotate_metrics.cypher" );

    public enum AnnotationTarget
    {
        ONLY_TEST_RUN,
        ONLY_METRICS,
        BOTH_TEST_RUN_AND_METRICS
    }

    private final long packagingBuildId;
    private final String comment;
    private final String author;
    private final String neo4jSeries;
    private final List<String> benchmarkTools;
    private final AnnotationTarget annotationTarget;

    public CreateAnnotations( long packagingBuildId,
                              String comment,
                              String author,
                              String neo4jSeries,
                              List<Repository> benchmarkTools,
                              AnnotationTarget annotationTarget )
    {
        this.packagingBuildId = packagingBuildId;
        this.comment = comment;
        this.author = author;
        this.neo4jSeries = neo4jSeries;
        this.annotationTarget = annotationTarget;
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
    public Void execute( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            if ( Lists.newArrayList( ONLY_TEST_RUN, BOTH_TEST_RUN_AND_METRICS ).contains( annotationTarget ) )
            {
                session.run( ANNOTATE_TEST_RUNS, params() ).consume();
            }
            if ( Lists.newArrayList( ONLY_METRICS, BOTH_TEST_RUN_AND_METRICS ).contains( annotationTarget ) )
            {
                session.run( ANNOTATE_METRICS, params() ).consume();
            }
        }
        return null;
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
        return "Params:\n" + prettyPrint( params() ) + ANNOTATE_TEST_RUNS;
    }

    @Override
    public Optional<String> nonFatalError()
    {
        return Optional.empty();
    }
}
