/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.neo4j.bench.client.model.TestRunError;
import com.neo4j.bench.client.model.TestRunReport;
import com.neo4j.bench.client.queries.SubmitTestRun;
import com.neo4j.bench.client.util.JsonUtil;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

import java.io.File;
import java.net.URI;
import java.util.List;

import static java.util.stream.Collectors.joining;

@Command( name = "report" )
public class ReportCommand implements Runnable
{
    public static final String CMD_RESULTS_STORE_USER = "--results_store_user";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RESULTS_STORE_USER},
            description = "Username for Neo4j database server that stores benchmarking results",
            title = "Results Store Username",
            required = true )
    private String resultsStoreUsername;

    public static final String CMD_RESULTS_STORE_PASSWORD = "--results_store_pass";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RESULTS_STORE_PASSWORD},
            description = "Password for Neo4j database server that stores benchmarking results",
            title = "Results Store Password",
            required = true )
    private String resultsStorePassword;

    public static final String CMD_RESULTS_STORE_URI = "--results_store_uri";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RESULTS_STORE_URI},
            description = "URI to Neo4j database server for storing benchmarking results",
            title = "Results Store",
            required = true )
    private URI resultsStoreUri;

    public static final String CMD_TEST_RUN_RESULTS = "--test_run_results";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TEST_RUN_RESULTS},
            description = "JSON file containing Test Run results",
            title = "JSON file containing Test Run results",
            required = false )
    private File testRunResultsJson;

    public enum ErrorReportingPolicy
    {
        // report regardless of errors. exit cleanly regardless of errors.
        IGNORE,
        // report regardless of errors. exit with error if report contains errors.
        REPORT_THEN_FAIL,
        // only report if there are no errors. exit with error if report contains errors.
        FAIL
    }

    static final String CMD_ERROR_POLICY = "--error-policy";
    @Option( type = OptionType.COMMAND,
            name = {CMD_ERROR_POLICY},
            description = "Error policy",
            title = "Error policy",
            required = false )
    private ErrorReportingPolicy errorReportingPolicy = ErrorReportingPolicy.REPORT_THEN_FAIL;

    @Override
    public void run()
    {
        try ( StoreClient client = StoreClient.connect( resultsStoreUri, resultsStoreUsername, resultsStorePassword ) )
        {
            TestRunReport testRunReport = JsonUtil.deserializeJson( testRunResultsJson.toPath(), TestRunReport.class );
            if ( errorReportingPolicy.equals( ErrorReportingPolicy.FAIL ) )
            {
                assertNoErrors( testRunReport );
            }
            SubmitTestRun submitTestRun = new SubmitTestRun( testRunReport );
            new QueryRetrier().execute( client, submitTestRun );
            System.out.println( "Successfully reported results" );
            if ( errorReportingPolicy.equals( ErrorReportingPolicy.REPORT_THEN_FAIL ) )
            {
                assertNoErrors( testRunReport );
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
