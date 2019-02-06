/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.neo4j.bench.client.model.BenchmarkGroupBenchmark;
import com.neo4j.bench.client.model.ProfileLoader;
import com.neo4j.bench.client.model.ProfilerRecordings;
import com.neo4j.bench.client.model.TestRunReport;
import com.neo4j.bench.client.util.JsonUtil;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Command( name = "add-profiles" )
public class AddProfilesCommand implements Runnable
{
    public static final String CMD_DIR = "--dir";
    @Option( type = OptionType.COMMAND,
            name = {CMD_DIR},
            description = "Directory that contains profile data",
            title = "Profiles data directory",
            required = true )
    private File profilesDir;

    public static final String CMD_TEST_RUN_RESULTS = "--test_run_report";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TEST_RUN_RESULTS},
            description = "JSON file containing Test Run Report",
            title = "Test Run Report file",
            required = true )
    private File testRunReportFile;

    public static final String CMD_S3_BUCKET = "--s3-bucket";
    @Option( type = OptionType.COMMAND,
            name = {CMD_S3_BUCKET},
            description = "S3 bucket profiles were uploaded to",
            title = "S3 bucket",
            required = true )
    private String s3Bucket;

    public static final String CMD_ARCHIVE = "--archive";
    @Option( type = OptionType.COMMAND,
            name = {CMD_ARCHIVE},
            description = "S3 path to archive file, uploading archive containing artifacts for entire test run",
            title = "S3 path to test run archive",
            required = false )
    private String s3ArchivePath;

    public static final String CMD_IGNORE_UNRECOGNIZED_FILES = "--ignore_unrecognized_files";
    @Option( type = OptionType.COMMAND,
            name = {CMD_IGNORE_UNRECOGNIZED_FILES},
            description = "If true will print warning on unrecognized profile recording, otherwise will fail",
            title = "Warn of unrecognized profile recordings",
            required = false )
    private boolean ignoreUnrecognizedFiles;

    @Override
    public void run()
    {
        try
        {
            TestRunReport testRunReport = JsonUtil.deserializeJson( testRunReportFile.toPath(), TestRunReport.class );

            Set<BenchmarkGroupBenchmark> benchmarkGroupBenchmarks = new HashSet<>( testRunReport.benchmarkGroupBenchmarks() );

            Map<BenchmarkGroupBenchmark,ProfilerRecordings> benchmarkProfiles = ProfileLoader.searchProfiles(
                    profilesDir.toPath(),
                    s3Bucket,
                    benchmarkGroupBenchmarks,
                    ignoreUnrecognizedFiles );

            benchmarkProfiles.forEach(
                    ( benchmark, profiles ) -> testRunReport.benchmarkGroupBenchmarkMetrics().attachProfilerRecording( benchmark.benchmarkGroup(),
                                                                                                                       benchmark.benchmark(),
                                                                                                                       profiles ) );

            if ( null != s3ArchivePath )
            {
                testRunReport.testRun().setArchive( s3ArchivePath );
            }

            JsonUtil.serializeJson( testRunReportFile.toPath(), testRunReport );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error encountered while trying to update test run report", e );
        }
    }
}
