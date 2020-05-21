/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.AllowedEnumValues;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.neo4j.bench.client.queries.submit.SubmitTestRun;
import com.neo4j.bench.model.model.TestRunError;
import com.neo4j.bench.model.model.TestRunReport;
import com.neo4j.bench.common.results.ErrorReportingPolicy;
import com.neo4j.bench.model.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.util.List;

import static java.util.stream.Collectors.joining;

@Command( name = "report" )
public class ReportCommand implements Runnable
{

    private static final Logger LOG = LoggerFactory.getLogger( ReportCommand.class );

    public static final String CMD_RESULTS_STORE_USER = "--results-store-user";
    @Option( type = OptionType.COMMAND,
             name = {CMD_RESULTS_STORE_USER},
             description = "Username for Neo4j database server that stores benchmarking results",
             title = "Results Store Username" )
    @Required
    private String resultsStoreUsername;

    public static final String CMD_RESULTS_STORE_PASSWORD = "--results-store-pass";
    @Option( type = OptionType.COMMAND,
             name = {CMD_RESULTS_STORE_PASSWORD},
             description = "Password for Neo4j database server that stores benchmarking results",
             title = "Results Store Password" )
    @Required
    private String resultsStorePassword;

    public static final String CMD_RESULTS_STORE_URI = "--results-store-uri";
    @Option( type = OptionType.COMMAND,
             name = {CMD_RESULTS_STORE_URI},
             description = "URI to Neo4j database server for storing benchmarking results",
             title = "Results Store" )
    @Required
    private URI resultsStoreUri;

    public static final String CMD_TEST_RUN_RESULTS = "--test_run_results";
    @Option( type = OptionType.COMMAND,
             name = {CMD_TEST_RUN_RESULTS},
             description = "JSON file containing Test Run results",
             title = "JSON file containing Test Run results" )
    private File testRunResultsJson;

    static final String CMD_ERROR_POLICY = "--error-policy";
    @Option( type = OptionType.COMMAND,
             name = {CMD_ERROR_POLICY},
             description = "Error policy",
             title = "Error policy" )
    @AllowedEnumValues( ErrorReportingPolicy.class )
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
            new QueryRetrier( true, QueryRetrier.DEFAULT_TIMEOUT ).execute( client, submitTestRun );
            LOG.debug( "Successfully reported results" );
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
