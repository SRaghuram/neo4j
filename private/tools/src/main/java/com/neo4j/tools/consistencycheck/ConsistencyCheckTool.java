/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.consistencycheck;

import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.index.label.RelationshipTypeScanStoreSettings;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.Stopwatch;

import static java.lang.String.format;
import static org.neo4j.configuration.Config.DEFAULT_CONFIG_FILE_NAME;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.experimental_consistency_checker;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.experimental_consistency_checker_stop_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.internal.helpers.Format.duration;

@CommandLine.Command( name = "consistency-check", description = "Runs consistency check on a database." )
public class ConsistencyCheckTool implements Callable<Object>
{
    @Spec
    protected CommandSpec spec;

    @CommandLine.Parameters( index = "0", description = "Home directory." )
    private File homeDirectory;

    @CommandLine.Option( names = "--database", description = "Database name." )
    private String databaseName = GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

    @CommandLine.Option( names = "--sample-size", description = "Number of lines to print of potential inconsistencies." )
    private int sampleSize = 50;

    @CommandLine.Option( names = "--config", description = "Config file to use, trying `home/" + DEFAULT_CONFIG_FILE_NAME + "` if unspecified" )
    private File configFile;

    @CommandLine.Option( names = {"--verbose", "-v"}, description = "Flag to enable a bit more debug outputs along the way" )
    private boolean verbose;

    @CommandLine.Option( names = {"--skip-indexes"}, description = "Flag to skip indexes" )
    private boolean skipIndexes;

    @CommandLine.Option( names = {"--skip-graph"}, description = "Flag to skip graph" )
    private boolean skipGraph;

    public static void main( String[] args )
    {
        System.exit( new CommandLine( new ConsistencyCheckTool() ).execute( args ) );
    }

    @Override
    public Object call() throws Exception
    {
        Config config = config();
        DatabaseLayout databaseLayout = DatabaseLayout.of( config );
        ConsistencyFlags flags = new ConsistencyFlags(
                !skipGraph,
                !skipIndexes,
                true, // feature unimplemented
                true, // feature-toggle unimplemented
                config.get( RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store ),
                false ); // feature unimplemented
        Stopwatch stopwatch = Stopwatch.start();
        ConsistencyCheckService.Result result =
                new ConsistencyCheckService().runFullConsistencyCheck( databaseLayout, config, ProgressMonitorFactory.textual( System.out ),
                        NullLogProvider.getInstance(), verbose, flags );
        System.out.println( format( "Check took %s database in '%s' %s", duration( stopwatch.elapsed( TimeUnit.MILLISECONDS ) ), homeDirectory,
                result.isSuccessful() ? "is consistent" : "has INCONSISTENCIES" ) );
        if ( !result.isSuccessful() )
        {
            System.err.println( "See inconsistencies report file in " + result.reportFile() );
            System.err.println( "Here is a sample of the first " + sampleSize + " lines:" );
            try ( BufferedReader reader = new BufferedReader( new FileReader( result.reportFile() ) ) )
            {
                int lines = 0;
                String line;
                while ( (line = reader.readLine()) != null && lines < sampleSize )
                {
                    System.err.println( line );
                    lines++;
                }

                if ( lines == sampleSize && reader.readLine() != null )
                {
                    System.err.println();
                    System.err.println( "... see report file for more inconsistencies, printing summary ..." );
                    System.err.println();
                    System.err.println( result.summary().toString() );
                }
            }
        }
        return null;
    }

    private Config config()
    {
        Config.Builder configBuilder = Config.newBuilder()
                .set( neo4j_home, homeDirectory.toPath().toAbsolutePath() )
                .setDefault( experimental_consistency_checker, true ) // default to using new checker
                .set( experimental_consistency_checker_stop_threshold, 80 )
                .fromFileNoThrow( configFile != null ? configFile : new File( homeDirectory, DEFAULT_CONFIG_FILE_NAME ) );
        if ( databaseName != null )
        {
            configBuilder.set( default_database, databaseName );
        }
        return configBuilder.build();
    }
}
