/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.harness;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.neo4j.logging.FormattedLogProvider.toOutputStream;

public class ClusterOfClustersInProcessRunner
{

    public static void main( String[] args )
    {
        try
        {
            Path clusterPath = Files.createTempDirectory( "causal-cluster" );
            System.out.println( "clusterPath = " + clusterPath );

            CausalClusterInProcessBuilder.CausalCluster cluster =
                    CausalClusterInProcessBuilder.init()
                            .withCores( 9 )
                            .withReplicas( 6 )
                            .withLogger( toOutputStream( System.out ) )
                            .atPath( clusterPath )
                            .withOptionalDatabases( Arrays.asList("foo", "bar", "baz") )
                            .build();

            System.out.println( "Waiting for cluster to boot up..." );
            cluster.boot();

            System.out.println( "Press ENTER to exit ..." );
            //noinspection ResultOfMethodCallIgnored
            System.in.read();

            System.out.println( "Shutting down..." );
            cluster.shutdown();
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
            System.exit( -1 );
        }
        System.exit( 0 );
    }

}
