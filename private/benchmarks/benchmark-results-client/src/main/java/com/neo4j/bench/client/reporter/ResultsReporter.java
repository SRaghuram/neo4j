/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.reporter;

import com.neo4j.bench.client.QueryRetrier;
import com.neo4j.bench.client.StoreClient;
import com.neo4j.bench.client.queries.submit.SubmitTestRun;
import com.neo4j.bench.common.results.ErrorReportingPolicy;
import com.neo4j.bench.model.model.TestRunError;
import com.neo4j.bench.model.model.TestRunReport;
import com.neo4j.bench.model.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.appendIfMissing;

public class ResultsReporter
{
    private static final Logger LOG = LoggerFactory.getLogger( ResultsReporter.class );

    private final String resultsStoreUsername;
    private final String resultsStorePassword;
    private final URI resultsStoreUri;
    private final String awsRegion = "eu-north-1";

    public ResultsReporter( String resultsStoreUsername,
                            String resultsStorePassword,
                            URI resultsStoreUri )
    {
        this.resultsStoreUsername = resultsStoreUsername;
        this.resultsStorePassword = resultsStorePassword;
        this.resultsStoreUri = resultsStoreUri;
    }

    public void reportAndUpload( TestRunReport testRunReport,
                                 URI recordingsBaseUri,
                                 File workDir,
                                 String awsEndpointURL,
                                 ErrorReportingPolicy errorReportingPolicy )
    {
        Objects.requireNonNull( recordingsBaseUri.getScheme(), "Recordings URI must contain scheme" );

        try
        {
            Path testRunReportFile = workDir.toPath().resolve( "test-result.json" );
            LOG.debug( "Exporting results as JSON to: {}", testRunReportFile.toAbsolutePath() );
            JsonUtil.serializeJson( testRunReportFile, testRunReport );

            String testRunId = testRunReport.testRun().id();
            LOG.debug( "Reporting test run with id {}", testRunId );

            Path tempRecordingsDir = Files.createTempDirectory( workDir.toPath(), null );
            URI s3ProfilerRecordingsFolderUri = AmazonS3Upload.constructS3Uri( recordingsBaseUri, tempRecordingsDir );

            String s3Folder = appendIfMissing( dropScheme( s3ProfilerRecordingsFolderUri ), "/" );
            ResultsCopy.extractProfilerRecordings( testRunReport.benchmarkGroupBenchmarkMetrics(),
                                                   tempRecordingsDir,
                                                   s3Folder,
                                                   workDir.toPath() );

            String archiveName = tempRecordingsDir.getFileName() + ".tar.gz";
            Path testRunArchive = workDir.toPath().resolve( archiveName );
            LOG.debug( "creating .tar.gz archive '{}' of profiles directory '{}'", testRunArchive, tempRecordingsDir );
            TarGzArchive.compress( testRunArchive, tempRecordingsDir );

            try ( AmazonS3Upload amazonS3Upload = AmazonS3Upload.create( awsRegion, awsEndpointURL ) )
            {
                LOG.debug( "uploading profiler recording directory '{}' to '{}'", tempRecordingsDir, s3ProfilerRecordingsFolderUri );
                amazonS3Upload.uploadFolder( tempRecordingsDir, s3ProfilerRecordingsFolderUri );

                URI testRunArchiveS3Uri = AmazonS3Upload.constructS3Uri( recordingsBaseUri, testRunArchive );
                LOG.debug( "uploading profiler recordings archive '{}' to '{}'", testRunArchive, testRunArchiveS3Uri );
                amazonS3Upload.uploadFile( testRunArchive, testRunArchiveS3Uri );
                String s3ArchivePath = dropScheme( testRunArchiveS3Uri );
                testRunReport.testRun().setArchive( s3ArchivePath );
            }

            report( testRunReport, errorReportingPolicy );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error submitting benchmark results to " + resultsStoreUri, e );
        }
    }

    private String dropScheme( URI s3Uri )
    {
        return s3Uri.getAuthority() + s3Uri.getPath();
    }

    public void report( TestRunReport testRunReport, ErrorReportingPolicy errorReportingPolicy )
    {
        try ( StoreClient client = StoreClient.connect( resultsStoreUri, resultsStoreUsername, resultsStorePassword ) )
        {
            if ( errorReportingPolicy.equals( ErrorReportingPolicy.FAIL ) )
            {
                assertNoErrors( testRunReport );
            }
            SubmitTestRun submitTestRun = new SubmitTestRun( testRunReport );
            new QueryRetrier( true, QueryRetrier.DEFAULT_TIMEOUT ).execute( client, submitTestRun );
            LOG.debug( "Successfully reported results" );
            if ( errorReportingPolicy.equals( ErrorReportingPolicy.REPORT_THEN_FAIL ) )
            {
                assertNoErrors( testRunReport );
            }
        }
    }

    private static void assertNoErrors( TestRunReport testRunReport )
    {
        List<TestRunError> errors = testRunReport.errors();
        if ( !errors.isEmpty() )
        {
            throw new RuntimeException( "==============================================================================================\n" +
                                        "Test Run Report Contained (" + errors.size() + ") Errors:\n" +
                                        errors.stream().map( e -> "\t" + e.benchmarkGroup().name() + e.benchmark().name() ).collect( joining( "\n" ) ) + "\n" +
                                        "==============================================================================================\n" +
                                        errors.stream()
                                              .map( TestRunError::message )
                                              .map( message -> message + "\n------------------------------------------------------------------------------" )
                                              .collect( joining( "\n" ) ) );
        }
    }
}
