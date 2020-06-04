/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

public class CustomSecurityInitializer
{
    private final Config config;
    private final Log log;

    public CustomSecurityInitializer( Config config, Log log )
    {
        this.config = config;
        this.log = log;
    }

    public void initialize( Transaction tx ) throws IOException
    {
        if ( config.isExplicitlySet( GraphDatabaseInternalSettings.system_init_file ) )
        {
            doCustomSecurityInitialization( tx );
        }
    }

    private void doCustomSecurityInitialization( Transaction tx ) throws IOException
    {
        // this is first startup and custom initialization specified
        File initFile = config.get( GraphDatabaseInternalSettings.system_init_file ).toFile();
        BufferedReader reader = new BufferedReader( new FileReader( initFile ) );
        String[] commands = reader.lines().filter( line -> !line.matches( "^\\s*//" ) ).collect( Collectors.joining( "\n" ) ).split( ";\\s*\n" );
        reader.close();
        for ( String command : commands )
        {
            if ( commandIsValid( command ) )
            {
                log.info( "Executing security initialization command: " + command );
                Result result = tx.execute( command );
                result.accept( new LoggingResultVisitor( result.columns() ) );
                result.close();
            }
            else
            {
                log.warn( "Ignoring invalid security initialization command: " + command );
            }
        }
    }

    private static boolean commandIsValid( String command )
    {
        return !command.matches( "^\\s*.*//" ) // Ignore comments
                && command.replaceAll( "\n", " " ).matches( "^\\s*\\w+.*" ); // Ignore blank lines
    }

    private class LoggingResultVisitor implements Result.ResultVisitor<RuntimeException>
    {
        private final List<String> columns;

        private LoggingResultVisitor( List<String> columns )
        {
            this.columns = columns;
        }

        @Override
        public boolean visit( Result.ResultRow row )
        {
            StringBuilder sb = new StringBuilder();
            for ( String column : columns )
            {
                if ( sb.length() > 0 )
                {
                    sb.append( ", " );
                }
                sb.append( column ).append( ":" ).append( row.get( column ).toString() );
            }
            log.info( "Result: " + sb.toString() );
            return true;
        }
    }
}
