/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.reporter;

import com.google.common.collect.Sets;
import com.neo4j.bench.client.QueryRetrier;
import com.neo4j.bench.client.StoreClient;
import com.neo4j.bench.client.queries.submit.SubmitTestRun;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ErrorReportingPolicy;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmark;
import com.neo4j.bench.model.model.TestRunError;
import com.neo4j.bench.model.model.TestRunReport;
import com.neo4j.bench.model.profiling.ProfilerRecordings;
import com.neo4j.bench.model.profiling.RecordingType;
import com.neo4j.bench.model.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.appendIfMissing;
import static org.apache.commons.lang3.StringUtils.removeStart;

public class ResultsReporter
{
    public static final String CMD_RESULTS_STORE_USER = "--results-store-user";
    public static final String CMD_RESULTS_STORE_PASSWORD = "--results-store-pass";
    public static final String CMD_RESULTS_STORE_URI = "--results-store-uri";

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
                                 Path profilesDir,
                                 String s3Bucket,
                                 File workDir,
                                 String awsEndpointURL,
                                 ErrorReportingPolicy errorReportingPolicy )
    {
        try
        {
            s3Bucket = appendIfMissing( s3Bucket, "/" );
            Path testRunReportFile = workDir.toPath().resolve( "test-result.json" );
            System.out.println( "Exporting results as JSON to: " + testRunReportFile.toAbsolutePath() );
            JsonUtil.serializeJson( testRunReportFile, testRunReport );

            String testRunId = testRunReport.testRun().id();
            System.out.println( format( "Reporting test run with id %s", testRunId ) );

            String archiveName = profilesDir.getFileName() + ".tar.gz";
            String s3ArchivePath = s3Bucket + archiveName;
            Path testRunArchive = workDir.toPath().resolve( archiveName );

            extractProfilerRecordings( testRunReport, profilesDir, s3Bucket, workDir );

            System.out.println( format( "creating .tar.gz archive '%s' of profiles directory '%s'", testRunArchive, profilesDir ) );
            TarGzArchive.compress( testRunArchive, profilesDir );

            try ( AmazonS3Upload amazonS3Upload = AmazonS3Upload.create( awsRegion, awsEndpointURL ) )
            {
                URI s3BucketURI = URI.create( StringUtils.prependIfMissing( s3Bucket, "s3://" ) );
                String bucketName = s3BucketURI.getAuthority();

                String keyPrefix = appendIfMissing( removeStart( s3BucketURI.getPath(), "/" ), "/" );
                System.out.println( format( "uploading profilers directory '%s' to '%s/%s'", profilesDir, bucketName, keyPrefix ) );

                UploadDirectoryToS3.execute( amazonS3Upload, bucketName, keyPrefix, profilesDir.toFile() );
                System.out
                        .println( format( "uploading profilers archive '%s' directory '%s' to '%s/%s'", testRunArchive, profilesDir, bucketName, keyPrefix ) );

                UploadFileToS3.execute( amazonS3Upload, bucketName, keyPrefix, testRunArchive );
                testRunReport.testRun().setArchive( s3ArchivePath );
            }

            report( testRunReport, errorReportingPolicy );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error submitting benchmark results to " + resultsStoreUri, e );
        }
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
            System.out.println( "Successfully reported results" );
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
                                        errors.stream().map( e -> "\t" + e.groupName() + e.benchmarkName() ).collect( joining( "\n" ) ) + "\n" +
                                        "==============================================================================================\n" +
                                        errors.stream()
                                              .map( TestRunError::message )
                                              .map( message -> message + "\n------------------------------------------------------------------------------" )
                                              .collect( joining( "\n" ) ) );
        }
    }

    /**
     * This method will:
     *  <ol>
     *   <li>Discovers profiler recordings in `workDir`</li>
     *   <li>Attach each discovered recording to the provided {@link TestRunReport}</li>
     *   <li>Copies each discovered recording to `tempProfilerRecordingsDir`</li>
     * </ol>
     */
    private void extractProfilerRecordings( TestRunReport testRunReport, Path tempProfilerRecordingsDir, String s3Bucket, File workDir )
    {
        Map<BenchmarkGroupBenchmark,ProfilerRecordings> benchmarkProfiles = new HashMap<>();
        Set<RecordingType> ignoredRecordingTypes = Sets.newHashSet( RecordingType.NONE,
                                                                    RecordingType.HEAP_DUMP,
                                                                    RecordingType.TRACE_STRACE,
                                                                    RecordingType.TRACE_MPSTAT,
                                                                    RecordingType.TRACE_VMSTAT,
                                                                    RecordingType.TRACE_IOSTAT,
                                                                    RecordingType.TRACE_JVM );

        for ( BenchmarkGroupDirectory benchmarkGroupDirectory : BenchmarkGroupDirectory.searchAllIn( workDir.toPath() ) )
        {
            for ( BenchmarkDirectory benchmarksDirectory : benchmarkGroupDirectory.benchmarkDirectories() )
            {
                BenchmarkGroupBenchmark benchmarkGroupBenchmark = new BenchmarkGroupBenchmark( benchmarkGroupDirectory.benchmarkGroup(),
                                                                                               benchmarksDirectory.benchmark() );
                // Only process successful benchmarks
                if ( !testRunReport.benchmarkGroupBenchmarks().contains( benchmarkGroupBenchmark ) )
                {
                    continue;
                }

                ProfilerRecordings profilerRecordings = new ProfilerRecordings();
                for ( ForkDirectory forkDirectory : benchmarksDirectory.forks() )
                {
                    forkDirectory.copyProfilerRecordings( tempProfilerRecordingsDir,
                                                          // only copy valid recordings for upload
                                                          recordingDescriptor -> !ignoredRecordingTypes.contains( recordingDescriptor.recordingType() ),
                                                          // attached valid/copied recordings to test run report
                                                          ( recordingDescriptor, recording ) ->
                                                                  profilerRecordings.with( recordingDescriptor.recordingType(),
                                                                                           recordingDescriptor.additionalParams(),
                                                                                           s3Bucket + recording.getFileName().toString() ) );
                }
                if ( !profilerRecordings.toMap().isEmpty() )
                {
                    // TODO once we have parameterized profilers we should assert that every expected recording exists
                    benchmarkProfiles.put( benchmarkGroupBenchmark, profilerRecordings );

                    // TODO do this instead?
//                    testRunReport.benchmarkGroupBenchmarkMetrics()
//                                 .attachProfilerRecording( benchmarkGroupBenchmark.benchmarkGroup(),
//                                                           benchmarkGroupBenchmark.benchmark(),
//                                                           profilerRecordings );
                }
            }
        }

        benchmarkProfiles.forEach( ( benchmark, profiles ) -> testRunReport.benchmarkGroupBenchmarkMetrics()
                                                                           .attachProfilerRecording( benchmark.benchmarkGroup(),
                                                                                                     benchmark.benchmark(),
                                                                                                     profiles ) );
    }
}
