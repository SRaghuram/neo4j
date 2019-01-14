/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.ha;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.Args;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class StartInstanceInAnotherJvm
{
    private StartInstanceInAnotherJvm()
    {
    }

    public static void main( String[] args )
    {
        File dir = new File( args[0] );
        GraphDatabaseAPI newSlave = (GraphDatabaseAPI) new TestHighlyAvailableGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( dir )
                .setConfig( Args.parse( args ).asMap() )
                .newGraphDatabase();
    }

    public static Process start( String dir, Map<String, String> config ) throws Exception
    {
        List<String> args = new ArrayList<>( Arrays.asList( "java", "-cp", System.getProperty( "java.class.path" ),
                StartInstanceInAnotherJvm.class.getName(), dir ) );
        for ( Map.Entry<String,String> property : config.entrySet() )
        {
            args.add( "-" + property.getKey() + "=" + property.getValue() );
        }
        return Runtime.getRuntime().exec( args.toArray( new String[ args.size() ] ) );
    }
}
