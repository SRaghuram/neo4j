/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.reporter;

import com.neo4j.bench.client.AddProfilesCommand;
import com.neo4j.bench.client.Main;
import com.neo4j.bench.client.QueryRetrier;
import com.neo4j.bench.client.StoreClient;
import com.neo4j.bench.client.queries.submit.SubmitTestRun;
import com.neo4j.bench.common.results.ErrorReportingPolicy;
import com.neo4j.bench.model.model.TestRunError;
import com.neo4j.bench.model.model.TestRunReport;
import com.neo4j.bench.model.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.appendIfMissing;
import static org.apache.commons.lang3.StringUtils.removeStart;

public class ResultsReporter
{

    private static final Logger LOG = LoggerFactory.getLogger( ResultsReporter.class );

    private final File profilesDir;
    private final TestRunReport testRunReport;
    private final String s3Bucket;
    private final boolean ignoreUnrecognizedFiles;
    private final String resultsStoreUsername;
    private final String resultsStorePassword;
    private final URI resultsStoreUri;
    private final ErrorReportingPolicy errorReportingPolicy = ErrorReportingPolicy.REPORT_THEN_FAIL;
    private final File workDir;
    private final String awsRegion = "eu-north-1";
    private final String awsEndpointURL;

    public ResultsReporter( File profilesDir,
                            TestRunReport testRunReport,
                            String s3Bucket,
                            boolean ignoreUnrecognizedFiles,
                            String resultsStoreUsername,
                            String resultsStorePassword,
                            URI resultsStoreUri,
                            File workDir,
                            String awsEndpointURL )
    {
        this.profilesDir = profilesDir;
        this.testRunReport = testRunReport;
        this.s3Bucket = s3Bucket;
        this.ignoreUnrecognizedFiles = ignoreUnrecognizedFiles;
        this.resultsStoreUsername = resultsStoreUsername;
        this.resultsStorePassword = resultsStorePassword;
        this.resultsStoreUri = resultsStoreUri;
        this.workDir = workDir;
        this.awsEndpointURL = awsEndpointURL;
    }

    public void report()
    {
        try
        {
            Path testRunReportFile = workDir.toPath().resolve( "test-result.json" );
            LOG.debug( "Exporting results as JSON to: " + testRunReportFile.toAbsolutePath() );
            JsonUtil.serializeJson( testRunReportFile, testRunReport );

            String testRunId = testRunReport.testRun().id();
            LOG.debug( format( "reporting test run with id %s", testRunId ) );

            String archiveName = profilesDir.getName() + ".tar.gz";
            Path testRunArchive = workDir.toPath().resolve( archiveName );
            String s3ArchivePath = appendIfMissing( s3Bucket, "/" ) + archiveName;
            LOG.debug( format( "add profiles for test run:\n" +
                                        "id=%s\n" +
                                        "testRunReportFile=%s\n" +
                                        "s3Bucket=%s\n" +
                                        "s3ArchivePath=%s\n" +
                                        "ignoreUnrecognizedFiles=%s", testRunId, testRunReportFile, s3Bucket, s3ArchivePath, ignoreUnrecognizedFiles ) );

            List<String> args = AddProfilesCommand.argsFor( profilesDir.toPath(),
                                                            testRunReportFile,
                                                            // Adding the UUID to the s3bucket for the profiling recordings
                                                            appendIfMissing( s3Bucket, "/" ) + profilesDir.getName(),
                                                            s3ArchivePath,
                                                            ignoreUnrecognizedFiles );
            Main.main( args.toArray( new String[args.size()] ) );

            TestRunReport testRunReportWithProfiles = JsonUtil.deserializeJson( testRunReportFile, TestRunReport.class );

            LOG.debug( format( "creating .tar.gz archive '%s' of profiles directory '%s'", testRunArchive, profilesDir ) );
            TarGzArchive.compress( testRunArchive, profilesDir.toPath() );

            try ( AmazonS3Upload amazonS3Upload = AmazonS3Upload.create( awsRegion, awsEndpointURL ) )
            {
                URI s3BucketURI = URI.create( StringUtils.prependIfMissing( s3Bucket, "s3://" ) );
                String bucketName = s3BucketURI.getAuthority();

                String keyPrefix = appendIfMissing( removeStart( s3BucketURI.getPath(), "/" ), "/" );
                LOG.debug( format( "uploading profilers directory '%s' to '%s/%s'", profilesDir, bucketName, keyPrefix ) );

                UploadDirectoryToS3.execute( amazonS3Upload, bucketName, keyPrefix, profilesDir );
                LOG.debug( format( "uploading profilers archive '%s' directory '%s' to '%s/%s'", testRunArchive, profilesDir, bucketName, keyPrefix ) );

                UploadFileToS3.execute( amazonS3Upload, bucketName, keyPrefix, testRunArchive );
                testRunReportWithProfiles.testRun().setArchive( appendIfMissing( s3Bucket, "/" ) + archiveName );
            }

            try ( StoreClient client = StoreClient.connect( resultsStoreUri, resultsStoreUsername, resultsStorePassword ) )
            {
                if ( errorReportingPolicy.equals( ErrorReportingPolicy.FAIL ) )
                {
                    assertNoErrors( testRunReportWithProfiles );
                }
                SubmitTestRun submitTestRun = new SubmitTestRun( testRunReportWithProfiles );
                new QueryRetrier( true, QueryRetrier.DEFAULT_TIMEOUT ).execute( client, submitTestRun );
                LOG.debug( "Successfully reported results" );
                if ( errorReportingPolicy.equals( ErrorReportingPolicy.REPORT_THEN_FAIL ) )
                {
                    assertNoErrors( testRunReportWithProfiles );
                }
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error submitting benchmark results to " + resultsStoreUri, e );
        }
    }

    private static void assertNoErrors( TestRunReport testRunReport )
    {
        List<TestRunError> errors = testRunReport.errors();
        if ( !errors.isEmpty() )
        {
            throw new RuntimeException( "==============================================================================================\n" +
                                        "Test Run Report Contained (" + errors.size() + ") Errors:\n" +
                                        errors.stream().map( e -> "\t" + e.groupName() + e.benchmarkName() ).collect( joining( "\n" ) ) + "\n" +
                                        "==============================================================================================\n" +
                                        errors.stream()
                                              .map( TestRunError::message )
                                              .map( message -> message + "\n------------------------------------------------------------------------------" )
                                              .collect( joining( "\n" ) ) );
        }
    }
}
