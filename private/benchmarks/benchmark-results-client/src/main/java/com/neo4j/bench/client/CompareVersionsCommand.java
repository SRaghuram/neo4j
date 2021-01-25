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
import com.google.common.collect.Lists;
import com.neo4j.bench.client.queries.Query;
import com.neo4j.bench.client.queries.report.CsvHeader;
import com.neo4j.bench.client.queries.report.CsvRow;
import com.neo4j.bench.client.queries.report.LdbcComparison;
import com.neo4j.bench.client.queries.report.MacroComparison;
import com.neo4j.bench.client.queries.report.MicroComparison;
import com.neo4j.bench.client.queries.report.MicroCoverage;
import com.neo4j.bench.common.util.BenchmarkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Command( name = "compare-versions" )
public class CompareVersionsCommand implements Runnable
{

    private static final Logger LOG = LoggerFactory.getLogger( CompareVersionsCommand.class );

    static final String MICRO_COMPARISON_FILENAME = "micro_comparison.csv";
    static final String MICRO_COVERAGE_FILENAME = "micro_coverage.csv";
    static final String LDBC_COMPARISON_FILENAME = "ldbc_comparison.csv";
    static final String MACRO_COMPARISON_FILENAME = "macro_comparison.csv";

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

    private static final String CMD_OLD_VERSION = "--old-version";
    @Option( type = OptionType.COMMAND,
             name = {CMD_OLD_VERSION},
             description = "Old (baseline) Neo4j version to compare against",
             title = "Old Version" )
    @Required
    private String oldNeo4jVersion;

    private static final String CMD_NEW_VERSION = "--new-version";
    @Option( type = OptionType.COMMAND,
             name = {CMD_NEW_VERSION},
             description = "New Neo4j version to compare to",
             title = "New Version" )
    @Required
    private String newNeo4jVersion;

    private static final String CMD_OUTPUT_DIRECTORY = "--output-dir";
    @Option( type = OptionType.COMMAND,
             name = {CMD_OUTPUT_DIRECTORY},
             description = "Directory to write .csv files to",
             title = "Output Directory" )
    @Required
    private File outputDir;

    private static final String CMD_MINIMUM_DIFFERENCE = "--min-diff";
    @Option( type = OptionType.COMMAND,
             name = {CMD_MINIMUM_DIFFERENCE},
             description = "Filter out results that have not changed by this factor, e.g., '1.2'",
             title = "Minimum Difference" )
    private double minimumDifference = 1;

    @Override
    public void run()
    {
        BenchmarkUtil.assertDirectoryExists( outputDir.toPath() );
        try ( StoreClient client = StoreClient.connect( resultsStoreUri, resultsStoreUsername, resultsStorePassword ) )
        {
            LOG.debug( "Writing results to: " + outputDir.getAbsolutePath() );

            toCsv( outputDir.toPath().resolve( MICRO_COMPARISON_FILENAME ),
                   client,
                   new MicroComparison( oldNeo4jVersion, newNeo4jVersion, minimumDifference ) );

            toCsv( outputDir.toPath().resolve( MICRO_COVERAGE_FILENAME ),
                   client,
                   new MicroCoverage( oldNeo4jVersion, newNeo4jVersion, minimumDifference ) );

            // TODO micro benchmark descriptions

            // TODO macro workload descriptions

            toCsv( outputDir.toPath().resolve( LDBC_COMPARISON_FILENAME ),
                   client,
                   new LdbcComparison( oldNeo4jVersion, newNeo4jVersion, minimumDifference ) );

            toCsv( outputDir.toPath().resolve( MACRO_COMPARISON_FILENAME ),
                   client,
                   new MacroComparison( oldNeo4jVersion, newNeo4jVersion, minimumDifference ) );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error collecting data", e );
        }
    }

    public static List<String> argsFor(
            String resultsStoreUsername,
            String resultsStorePassword,
            URI resultsStoreUri,
            String oldNeo4jVersion,
            String newNeo4jVersion,
            double minimumDifference,
            Path outputDir )
    {
        return Lists.newArrayList( "compare-versions",
                                   CMD_RESULTS_STORE_USER,
                                   resultsStoreUsername,
                                   CMD_RESULTS_STORE_PASSWORD,
                                   resultsStorePassword,
                                   CMD_RESULTS_STORE_URI,
                                   resultsStoreUri.toString(),
                                   CMD_OLD_VERSION,
                                   oldNeo4jVersion,
                                   CMD_NEW_VERSION,
                                   newNeo4jVersion,
                                   CMD_MINIMUM_DIFFERENCE,
                                   Double.toString( minimumDifference ),
                                   CMD_OUTPUT_DIRECTORY,
                                   outputDir.toAbsolutePath().toString() );
    }

    private <CSV_ROW extends CsvRow, QUERY extends Query<List<CSV_ROW>> & CsvHeader> void toCsv( Path csvFile,
                                                                                                 StoreClient client,
                                                                                                 QUERY query ) throws IOException
    {
        BenchmarkUtil.assertDoesNotExist( csvFile );
        Files.createFile( csvFile );
        BenchmarkUtil.assertFileExists( csvFile );
        try ( PrintWriter csvWriter = new PrintWriter( Files.newOutputStream( csvFile ), true /*auto flush*/ ) )
        {
            csvWriter.println( query.header() );
            List<CSV_ROW> csvRows = client.execute( query );
            if ( csvRows.isEmpty() )
            {
                throw new RuntimeException( "Results were unexpectedly empty!\n" +
                                            "Can not create: " + csvFile.toAbsolutePath().toString() );
            }
            csvRows.forEach( csvWriter::println );
        }
    }
}
