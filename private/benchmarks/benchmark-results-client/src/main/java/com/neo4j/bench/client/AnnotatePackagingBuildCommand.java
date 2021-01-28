/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.ImmutableList;
import com.neo4j.bench.client.queries.annotation.CreateAnnotations;
import com.neo4j.bench.client.queries.annotation.CreateAnnotations.AnnotationTarget;
import com.neo4j.bench.client.queries.annotation.CreateAnnotationsResult;
import com.neo4j.bench.common.command.ResultsStoreArgs;
import com.neo4j.bench.model.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;

import static com.neo4j.bench.client.queries.annotation.CreateAnnotations.AnnotationTarget.METRICS;
import static com.neo4j.bench.client.queries.annotation.CreateAnnotations.AnnotationTarget.TEST_RUN;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@Command( name = "packaging" )
public class AnnotatePackagingBuildCommand implements Runnable
{
    private static final Logger LOG = LoggerFactory.getLogger( AnnotatePackagingBuildCommand.class );

    @Inject
    private final ResultsStoreArgs resultsStoreArgs = new ResultsStoreArgs();

    private static final String CMD_PACKAGING_BUILD_ID = "--packaging-build-id";
    @Option( type = OptionType.COMMAND,
             name = {CMD_PACKAGING_BUILD_ID},
             description = "ID of packaging build that contained the commit to annotate",
             title = "Packaging Build ID" )
    @Required
    private long packagingBuildId;

    private static final String CMD_ANNOTATION_COMMENT = "--annotation-comment";
    @Option( type = OptionType.COMMAND,
             name = {CMD_ANNOTATION_COMMENT},
             description = "Annotation comment",
             title = "Annotation Comment" )
    @Required
    private String annotationComment;

    private static final String CMD_ANNOTATION_AUTHOR = "--annotation-author";
    @Option( type = OptionType.COMMAND,
             name = {CMD_ANNOTATION_AUTHOR},
             description = "Annotation author",
             title = "Annotation Author" )
    @Required
    private String annotationAuthor;

    private static final String CMD_NEO4J_SERIES = "--neo4j-series";
    @Option( type = OptionType.COMMAND,
             name = {CMD_NEO4J_SERIES},
             description = "Neo4j Series, e.g., '3.4', '3.5', etc.",
             title = "Annotation Series" )
    @Required
    private String neo4jSeries;

    private static final String CMD_BENCHMARK_TOOLS = "--benchmark-tools";
    @Option( type = OptionType.COMMAND,
             name = {CMD_BENCHMARK_TOOLS},
             description = "Annotates the latest test run for each benchmark tool in this comma-separated list, e.g., 'micro,ldbc,macro'",
             title = "Benchmark Tools" )
    private String benchmarkToolNames;

    private static final String CMD_ANNOTATE_TEST_RUNS = "--annotate-test-runs";
    @Option( type = OptionType.COMMAND,
             name = {CMD_ANNOTATE_TEST_RUNS},
             description = "If flag is set, annotations will be created on :TestRun nodes",
             title = "Annotate Test Runs" )
    private boolean doTestRunAnnotations;

    private static final String CMD_ANNOTATE_METRICS = "--annotate-metrics";
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
        try ( StoreClient client = StoreClient.connect( resultsStoreArgs.resultsStoreUri(),
                                                        resultsStoreArgs.resultsStoreUsername(),
                                                        resultsStoreArgs.resultsStorePassword() ) )
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
                                                             getAnnotationTargets() );
            CreateAnnotationsResult result = client.execute( query );
            LOG.debug( format( "Annotations created!\n" +
                               "Test Run Annotations: %s\n" +
                               "Metrics Annotations:  %s",
                               result.createdTestRunAnnotations(),
                               result.createdMetricsAnnotations() ) );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error creating annotations", e );
        }
    }

    private Set<AnnotationTarget> getAnnotationTargets()
    {
        Set<AnnotationTarget> annotationTargets = new HashSet<>();
        if ( doTestRunAnnotations )
        {
            annotationTargets.add( TEST_RUN );
        }
        if ( doMetricsAnnotations )
        {
            annotationTargets.add( METRICS );
        }
        return annotationTargets;
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
            Set<AnnotationTarget> annotationTargets )
    {
        ImmutableList.Builder<String> args = ImmutableList.<String>builder()
                .add( "annotate",
                      "packaging",
                      CMD_PACKAGING_BUILD_ID,
                      Long.toString( packagingBuildId ),
                      CMD_ANNOTATION_COMMENT,
                      annotationComment,
                      CMD_ANNOTATION_AUTHOR,
                      annotationAuthor,
                      CMD_NEO4J_SERIES,
                      neo4jSeries )
                .addAll( ResultsStoreArgs.argsFor( resultsStoreUsername, resultsStorePassword, resultsStoreUri ) );

        if ( !benchmarkTools.isEmpty() )
        {
            args.add( CMD_BENCHMARK_TOOLS );
            args.add( benchmarkTools.stream().map( Repository::projectName ).collect( joining( "," ) ) );
        }
        if ( annotationTargets.contains( TEST_RUN ) )
        {
            args.add( CMD_ANNOTATE_TEST_RUNS );
        }
        if ( annotationTargets.contains( METRICS ) )
        {
            args.add( CMD_ANNOTATE_METRICS );
        }
        return args.build();
    }
}
