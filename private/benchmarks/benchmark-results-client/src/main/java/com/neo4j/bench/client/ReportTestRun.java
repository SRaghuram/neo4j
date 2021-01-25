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
import com.neo4j.bench.client.reporter.ResultsReporter;
import com.neo4j.bench.common.results.ErrorReportingPolicy;
import com.neo4j.bench.model.model.TestRunReport;
import com.neo4j.bench.model.util.JsonUtil;

import java.io.File;
import java.net.URI;

@Command( name = "report-test-run" )
public class ReportTestRun implements Runnable
{

    private static final String CMD_RESULTS_STORE_USER = "--results-store-user";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RESULTS_STORE_USER},
            description = "Username for Neo4j database server that stores benchmarking results",
            title = "Results Store Username" )
    @Required
    private String resultsStoreUsername;

    private static final String CMD_RESULTS_STORE_PASSWORD = "--results-store-pass";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RESULTS_STORE_PASSWORD},
            description = "Password for Neo4j database server that stores benchmarking results",
            title = "Results Store Password" )
    @Required
    private String resultsStorePassword;

    private static final String CMD_RESULTS_STORE_URI = "--results-store-uri";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RESULTS_STORE_URI},
            description = "URI to Neo4j database server for storing benchmarking results",
            title = "Results Store" )
    @Required
    private URI resultsStoreUri;

    private static final String CMD_TEST_RUN_REPORT_FILE = "--test-run-report";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TEST_RUN_REPORT_FILE},
            description = "Test run report JSON file",
            title = "Test run report" )
    @Required
    private File testRunReportFile;

    private static final String CMD_S3_BUCKET = "--s3-bucket";
    @Option( type = OptionType.COMMAND,
            name = {CMD_S3_BUCKET},
            description = "S3 bucket for uploading profiler recordings",
            title = "S3 bucket" )
    @Required
    private String s3Bucket;

    private static final String CMD_WORK_DIR = "--working-dir";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WORK_DIR},
            description = "Working directory where we will lookup up for profiler recordings",
            title = "Working Directory" )
    @Required
    private File workDir;

    private static final String CMD_AWS_ENDPOINT_URL = "--aws-endpoint-url";
    @Option( type = OptionType.COMMAND,
            name = {CMD_AWS_ENDPOINT_URL},
            description = "AWS endpoint URL, used during testing",
            title = "AWS endpoint URL" )
    private String awsEndpointURL;

    private static final String CMD_ERROR_REPORTING_POLICY = "--error-reporting";
    @Option( type = OptionType.COMMAND,
            name = {CMD_ERROR_REPORTING_POLICY},
            description = "Error reporting policy",
            title = "Error reporting policy" )
    private ErrorReportingPolicy errorReportingPolicy = ErrorReportingPolicy.REPORT_THEN_FAIL;

    @Override
    public void run()
    {
        TestRunReport testRunReport = JsonUtil.deserializeJson( testRunReportFile.toPath(), TestRunReport.class );
        ResultsReporter resultsReporter = new ResultsReporter( resultsStoreUsername, resultsStorePassword, resultsStoreUri );
        resultsReporter.reportAndUpload( testRunReport, s3Bucket, workDir, awsEndpointURL, errorReportingPolicy );
    }
}
