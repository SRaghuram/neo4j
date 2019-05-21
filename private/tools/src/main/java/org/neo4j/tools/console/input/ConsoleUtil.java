/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.console.input;

import io.airlift.airline.Cli;
import io.airlift.airline.Help;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.StringTokenizer;

import org.neo4j.io.NullOutputStream;
import org.neo4j.kernel.impl.util.Listener;

import static org.neo4j.internal.helpers.ArrayUtil.join;

public class ConsoleUtil
{
    public static final Listener<PrintStream> NO_PROMPT = out ->
    {   // Do nothing
    };

    public static Listener<PrintStream> staticPrompt( final String prompt )
    {
        return out -> out.print( prompt );
    }

    public static final PrintStream NULL_PRINT_STREAM = new PrintStream( NullOutputStream.NULL_OUTPUT_STREAM );

    public static String[] tokenizeStringWithQuotes( String string )
    {
        ArrayList<String> result = new ArrayList<>();
        string = string.trim();
        boolean inside = string.startsWith( "\"" );
        StringTokenizer quoteTokenizer = new StringTokenizer( string, "\"" );
        while ( quoteTokenizer.hasMoreTokens() )
        {
            String token = quoteTokenizer.nextToken();
            token = token.trim();
            if ( token.length() == 0 )
            {
                // Skip it
            }
            else if ( inside )
            {
                // Don't split
                result.add( token );
            }
            else
            {
                splitAndKeepEscapedSpaces( token, false, result );
            }
            inside = !inside;
        }
        return result.toArray( new String[result.size()] );
    }

    private static void splitAndKeepEscapedSpaces( String string, boolean preserveEscapes, Collection<String> into )
    {
        StringBuilder current = new StringBuilder();
        for ( int i = 0; i < string.length(); i++ )
        {
            char ch = string.charAt( i );
            if ( ch == ' ' )
            {
                boolean isGluedSpace = i > 0 && string.charAt( i - 1 ) == '\\';
                if ( !isGluedSpace )
                {
                    into.add( current.toString() );
                    current = new StringBuilder();
                    continue;
                }
            }

            if ( preserveEscapes || ch != '\\' )
            {
                current.append( ch );
            }
        }
        if ( current.length() > 0 )
        {
            into.add( current.toString() );
        }
    }

    public static InputStream oneCommand( String... args )
    {
        String string = join( args, " " );
        return new ByteArrayInputStream( string.getBytes() );
    }

    public static String airlineHelp( Cli<?> cli )
    {
        StringBuilder builder = new StringBuilder();
        Help.help( cli.getMetadata(), Collections.emptyList(), builder );
        return builder.toString();
    }

    private ConsoleUtil()
    {
    }
}
