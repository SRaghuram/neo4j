/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.Lists;
import com.neo4j.bench.client.queries.report.MicroComparison;
import com.neo4j.bench.client.queries.report.MicroComparisonResult;
import com.neo4j.bench.common.util.BenchmarkUtil;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.util.List;

import static java.lang.String.format;

@Command( name = "compare-versions" )
public class CompareVersionsCommand implements Runnable
{
    private static final String CMD_RESULTS_STORE_USER = "--results_store_user";
    @Option( type = OptionType.COMMAND,
             name = {CMD_RESULTS_STORE_USER},
             description = "Username for Neo4j database server that stores benchmarking results",
             title = "Results Store Username" )
    @Required
    private String resultsStoreUsername;

    private static final String CMD_RESULTS_STORE_PASSWORD = "--results_store_pass";
    @Option( type = OptionType.COMMAND,
             name = {CMD_RESULTS_STORE_PASSWORD},
             description = "Password for Neo4j database server that stores benchmarking results",
             title = "Results Store Password" )
    @Required
    private String resultsStorePassword;

    private static final String CMD_RESULTS_STORE_URI = "--results_store_uri";
    @Option( type = OptionType.COMMAND,
             name = {CMD_RESULTS_STORE_URI},
             description = "URI to Neo4j database server for storing benchmarking results",
             title = "Results Store" )
    @Required
    private URI resultsStoreUri;

    private static final String CMD_OLD_VERSION = "--old_version";
    @Option( type = OptionType.COMMAND,
             name = {CMD_OLD_VERSION},
             description = "Old (baseline) Neo4j version to compare against",
             title = "Old Version" )
    @Required
    private String oldNeo4jVersion;

    private static final String CMD_NEW_VERSION = "--new_version";
    @Option( type = OptionType.COMMAND,
             name = {CMD_NEW_VERSION},
             description = "New Neo4j version to compare to",
             title = "New Version" )
    @Required
    private String newNeo4jVersion;

    private static final String CMD_OUTPUT_DIRECTORY = "--output_dir";
    @Option( type = OptionType.COMMAND,
             name = {CMD_OUTPUT_DIRECTORY},
             description = "Directory to write .csv files to",
             title = "Output Directory" )
    @Required
    private File outputDir;

    private static final String CMD_MINIMUM_DIFFERENCE = "--min_diff";
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
            System.out.println( "Writing results to: " + outputDir.getAbsolutePath() );
            toCsv( outputDir.toPath(),
                   client.execute( new MicroComparison( oldNeo4jVersion, newNeo4jVersion, minimumDifference ) ) );

            // TODO many other comparisons

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

    private void toCsv( Path outputDir, List<MicroComparisonResult> results ) throws IOException
    {
        Path csvFile = Files.createFile( outputDir.resolve( "micro_comparison.csv" ) );
        BenchmarkUtil.assertFileExists( csvFile );
        try ( PrintWriter csvWriter = new PrintWriter( Files.newOutputStream( csvFile ), true /*auto flush*/ ) )
        {
            String header = format( "group,bench,%s,%s,unit,improvement", oldNeo4jVersion, newNeo4jVersion );
            csvWriter.println( header );
            DecimalFormat numberFormatter = new DecimalFormat( "#.00" );
            results.forEach( result ->
                             {
                                 String line = format( "%s,%s,%s,%s,%s,%s",
                                                       result.group(),
                                                       result.bench().replace( ",", ":" ),
                                                       numberFormatter.format( result.oldResult() ),
                                                       numberFormatter.format( result.newResult() ),
                                                       result.unit(),
                                                       numberFormatter.format( result.improvement() )
                                 );
                                 csvWriter.println( line );
                             } );
        }
    }
}
