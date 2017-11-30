/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.unsafe.impl.batchimport.staging;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.helpers.collection.Pair;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class InputCommands extends ExecutionMonitor.Adapter
{
    private final Map<String,Pair<String,Consumer<String>>> actions = new HashMap<>();
    private final PrintStream out;

    public InputCommands( PrintStream out )
    {
        super( 500, MILLISECONDS );
        this.out = out;
    }

    public void add( String name, String description, Consumer<String> action )
    {
        this.actions.put( name, Pair.of( description, action ) );
    }

    public void reactToUserInput()
    {
        try
        {
            if ( System.in.available() > 0 )
            {
                // don't close this read, since we really don't want to close the underlying System.in
                BufferedReader reader = new BufferedReader( new InputStreamReader( System.in ) );
                String line = reader.readLine();
                String[] parts = line.split( " " );
                Pair<String,Consumer<String>> action = actions.get( parts[0] );
                if ( action != null )
                {
                    action.other().accept( line );
                }
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace( out );
        }
    }

    public void printActions()
    {
        actions.forEach( ( key, action ) -> out.println( "  " + key + ": " + action.first() ) );
    }

    @Override
    public void check( StageExecution execution )
    {
        reactToUserInput();
    }
}
