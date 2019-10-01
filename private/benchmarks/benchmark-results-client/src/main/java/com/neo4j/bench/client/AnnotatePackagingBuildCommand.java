/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.Lists;
import com.neo4j.bench.client.queries.annotation.CreateAnnotations;
import com.neo4j.bench.client.queries.annotation.CreateAnnotations.AnnotationTarget;
import com.neo4j.bench.common.model.Repository;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@Command( name = "packaging" )
public class AnnotatePackagingBuildCommand implements Runnable
{
    private static final String CMD_RESULTS_STORE_USER = "--results_store_user";
    @Option( type = OptionType.COMMAND,
             name = {CMD_RESULTS_STORE_USER},
             description = "Username for Neo4j database server that stores benchmarking results",
             title = "Results Store Username" )
    @Required
    private String resultsStoreUsername;

    private static final String CMD_RESULTS_STORE_PASSWORD = "--results_store_pass";
    @Option( type = OptionType.COMMAND,
             name = {CMD_RESULTS_STORE_PASSWORD},
             description = "Password for Neo4j database server that stores benchmarking results",
             title = "Results Store Password" )
    @Required
    private String resultsStorePassword;

    private static final String CMD_RESULTS_STORE_URI = "--results_store_uri";
    @Option( type = OptionType.COMMAND,
             name = {CMD_RESULTS_STORE_URI},
             description = "URI to Neo4j database server for storing benchmarking results",
             title = "Results Store" )
    @Required
    private URI resultsStoreUri;

    private static final String CMD_PACKAGING_BUILD_ID = "--packaging_build_id";
    @Option( type = OptionType.COMMAND,
             name = {CMD_PACKAGING_BUILD_ID},
             description = "ID of packaging build that contained the commit to annotate",
             title = "Packaging Build ID" )
    @Required
    private long packagingBuildId;

    private static final String CMD_ANNOTATION_COMMENT = "--annotation_comment";
    @Option( type = OptionType.COMMAND,
             name = {CMD_ANNOTATION_COMMENT},
             description = "Annotation comment",
             title = "Annotation Comment" )
    @Required
    private String annotationComment;

    private static final String CMD_ANNOTATION_AUTHOR = "--annotation_author";
    @Option( type = OptionType.COMMAND,
             name = {CMD_ANNOTATION_AUTHOR},
             description = "Annotation author",
             title = "Annotation Author" )
    @Required
    private String annotationAuthor;

    private static final String CMD_NEO4J_SERIES = "--neo4j_series";
    @Option( type = OptionType.COMMAND,
             name = {CMD_NEO4J_SERIES},
             description = "Neo4j Series, e.g., '3.4', '3.5', etc.",
             title = "Annotation Series" )
    @Required
    private String neo4jSeries;

    private static final String CMD_BENCHMARK_TOOLS = "--benchmark_tools";
    @Option( type = OptionType.COMMAND,
             name = {CMD_BENCHMARK_TOOLS},
             description = "Annotates the latest test run for each benchmark tool in this comma-separated list, e.g., 'micro,ldbc,macro'",
             title = "Benchmark Tools" )
    private String benchmarkToolNames;

    private static final String CMD_ANNOTATE_TEST_RUNS = "--annotate_test_runs";
    @Option( type = OptionType.COMMAND,
             name = {CMD_ANNOTATE_TEST_RUNS},
             description = "If flag is set, annotations will be created on :TestRun nodes",
             title = "Annotate Test Runs" )
    private boolean doTestRunAnnotations;

    private static final String CMD_ANNOTATE_METRICS = "--annotate_metrics";
    @Option( type = OptionType.COMMAND,
             name = {CMD_ANNOTATE_METRICS},
             description = "If flag is set, annotations will be created on :Metrics nodes",
             title = "Annotate Test Runs" )
    private boolean doMetricsAnnotations;

    @Override
    public void run()
    {
        if ( !Repository.NEO4J.isStandardBranch( neo4jSeries ) )
        {
            throw new IllegalArgumentException( "Command only supports annotating standard Neo4j branches. Branch is not standard: " + neo4jSeries );
        }
        try ( StoreClient client = StoreClient.connect( resultsStoreUri, resultsStoreUsername, resultsStorePassword ) )
        {
            List<Repository> benchmarkTools = benchmarkToolNames == null
                                              // create annotation for all (supported) tools
                                              ? Arrays.stream( Repository.values() ).filter( CreateAnnotations::isSupportedTool ).collect( toList() )
                                              // create annotation for selected tools
                                              : Arrays.stream( benchmarkToolNames.split( "," ) ).map( Repository::forName ).collect( toList() );
            CreateAnnotations query = new CreateAnnotations( packagingBuildId,
                                                             annotationComment,
                                                             annotationAuthor,
                                                             neo4jSeries,
                                                             benchmarkTools,
                                                             getAnnotationTarget() );
            client.execute( query );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error creating annotations", e );
        }
    }

    private AnnotationTarget getAnnotationTarget()
    {
        if ( doTestRunAnnotations && doMetricsAnnotations )
        {
            return AnnotationTarget.BOTH_TEST_RUN_AND_METRICS;
        }
        else if ( !doTestRunAnnotations && !doMetricsAnnotations )
        {
            throw new IllegalStateException( "Must annotate at least one of: TestRun, Metrics" );
        }
        else if ( doTestRunAnnotations )
        {
            return AnnotationTarget.ONLY_TEST_RUN;
        }
        else
        {
            return AnnotationTarget.ONLY_METRICS;
        }
    }

    public static List<String> argsFor(
            String resultsStoreUsername,
            String resultsStorePassword,
            URI resultsStoreUri,
            long packagingBuildId,
            String annotationComment,
            String annotationAuthor,
            String neo4jSeries,
            List<Repository> benchmarkTools,
            AnnotationTarget annotationTarget )
    {
        ArrayList<String> args = Lists.newArrayList( "annotate",
                                                     "packaging",
                                                     CMD_RESULTS_STORE_USER,
                                                     resultsStoreUsername,
                                                     CMD_RESULTS_STORE_PASSWORD,
                                                     resultsStorePassword,
                                                     CMD_RESULTS_STORE_URI,
                                                     resultsStoreUri.toString(),
                                                     CMD_PACKAGING_BUILD_ID,
                                                     Long.toString( packagingBuildId ),
                                                     CMD_ANNOTATION_COMMENT,
                                                     annotationComment,
                                                     CMD_ANNOTATION_AUTHOR,
                                                     annotationAuthor,
                                                     CMD_NEO4J_SERIES,
                                                     neo4jSeries );
        if ( !benchmarkTools.isEmpty() )
        {
            args.add( CMD_BENCHMARK_TOOLS );
            args.add( benchmarkTools.stream().map( Repository::projectName ).collect( joining( "," ) ) );
        }
        switch ( annotationTarget )
        {
        case BOTH_TEST_RUN_AND_METRICS:
            args.add( CMD_ANNOTATE_TEST_RUNS );
            args.add( CMD_ANNOTATE_METRICS );
            break;
        case ONLY_TEST_RUN:
            args.add( CMD_ANNOTATE_TEST_RUNS );
            break;
        case ONLY_METRICS:
            args.add( CMD_ANNOTATE_METRICS );
            break;
        }
        return args;
    }
}
