/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.internal.helpers.Args;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class CommandReactor
{
    private final Map<String,Action> actions = new HashMap<>();
    private final Reactor reactor;
    private final Log log;
    private final Action helpAction = new Action()
    {
        @Override
        public void run( Args action )
        {
            log.info( "Listing available commands:" );
            for ( Map.Entry<String,Action> entry : actions.entrySet() )
            {
                log.info( "  " + entry.getKey() + ": " + entry.getValue() );
            }
        }
    };

    public CommandReactor( String name, LogProvider logProvider )
    {
        this.reactor = new Reactor( name );
        this.log = logProvider.getLog( getClass() );
    }

    public void add( String command, Action action )
    {
        this.actions.put( command, action );
    }

    public void shutdown()
    {
        reactor.halted = true;
    }

    public boolean isShutdown()
    {
        return !reactor.isAlive();
    }

    public void waitFor() throws InterruptedException
    {
        reactor.join();
    }

    public interface Action
    {
        void run( Args action ) throws Exception;
    }

    private class Reactor extends Thread
    {
        private final Path commandFile = Path.of( "command" );
        private volatile boolean halted;

        Reactor( String name )
        {
            super( name + " - terminal command reactor" );
            start();
        }

        @Override
        public void run()
        {
            while ( !halted )
            {
                try
                {
                    String commandLine = getAvailableCommand();
                    if ( commandLine != null )
                    {
//                        log.info( "TYPED '" + command + "'" );
                        String command = extractCommand( commandLine );
                        Action action = getAction( command );
                        if ( action != null )
                        {
                            action.run( Args.parse( commandLine.split( " " ) ) );
                        }
//                        log.info( "DONE" );
                    }
                    else
                    {
                        Thread.sleep( 200 );
                    }
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                    // The show must go on
                }
            }
        }

        private Action getAction( String command )
        {
            if ( command.equals( "?" ) )
            {
                return helpAction;
            }
            return actions.get( command );
        }

        private String extractCommand( String commandLine )
        {
            int index = commandLine.indexOf( ' ' );
            return index == -1 ? commandLine : commandLine.substring( 0, index );
        }

        private String getAvailableCommand() throws IOException
        {
            if ( System.in.available() > 0 )
            {
                String line = new BufferedReader( new InputStreamReader( System.in ) ).readLine();
                if ( line != null )
                {
                    return line;
                }
            }
            if ( Files.exists( commandFile ) )
            {
                String line;
                try ( BufferedReader reader = Files.newBufferedReader( commandFile ) )
                {
                    line = reader.readLine();
                }
                Files.delete( commandFile );
                return line;
            }
            return null;
        }
    }
}
