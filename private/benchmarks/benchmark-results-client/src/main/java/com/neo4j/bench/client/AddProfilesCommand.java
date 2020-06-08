/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.Lists;
import com.neo4j.bench.common.profiling.ProfileLoader;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmark;
import com.neo4j.bench.model.model.TestRunReport;
import com.neo4j.bench.model.profiling.ProfilerRecordings;
import com.neo4j.bench.model.util.JsonUtil;

import java.io.File;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Command( name = "add-profiles" )
public class AddProfilesCommand implements Runnable
{
    public static final String CMD_DIR = "--dir";
    @Option( type = OptionType.COMMAND,
            name = {CMD_DIR},
            description = "Directory that contains profile data",
            title = "Profiles data directory" )
    @Required
    private File profilesDir;

    public static final String CMD_TEST_RUN_RESULTS = "--test_run_report";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TEST_RUN_RESULTS},
            description = "JSON file containing Test Run Report",
            title = "Test Run Report file" )
    @Required
    private File testRunReportFile;

    public static final String CMD_S3_BUCKET = "--s3-bucket";
    @Option( type = OptionType.COMMAND,
            name = {CMD_S3_BUCKET},
            description = "S3 bucket profiles were uploaded to",
            title = "S3 bucket" )
    @Required
    private String s3Bucket;

    public static final String CMD_ARCHIVE = "--archive";
    @Option( type = OptionType.COMMAND,
            name = {CMD_ARCHIVE},
            description = "S3 path to archive file, uploading archive containing artifacts for entire test run",
            title = "S3 path to test run archive" )
    private String s3ArchivePath;

    public static final String CMD_IGNORE_UNRECOGNIZED_FILES = "--ignore_unrecognized_files";
    @Option( type = OptionType.COMMAND,
            name = {CMD_IGNORE_UNRECOGNIZED_FILES},
            description = "If true will print warning on unrecognized profile recording, otherwise will fail",
            title = "Warn of unrecognized profile recordings" )
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

    public static List<String> argsFor( Path profilesDir,
                                        Path testRunReportJson,
                                        String s3BucketPath,
                                        String s3ArchivePath,
                                        boolean ignoreUnrecognizedFiles )
    {
        List<String> args = Lists.newArrayList(
                "add-profiles",
                AddProfilesCommand.CMD_DIR, profilesDir.toAbsolutePath().toString(),
                AddProfilesCommand.CMD_TEST_RUN_RESULTS, testRunReportJson.toAbsolutePath().toString(),
                AddProfilesCommand.CMD_S3_BUCKET, s3BucketPath,
                AddProfilesCommand.CMD_ARCHIVE, s3ArchivePath );
        if ( ignoreUnrecognizedFiles )
        {
            args.add( AddProfilesCommand.CMD_IGNORE_UNRECOGNIZED_FILES );
        }
        return args;
    }
}
