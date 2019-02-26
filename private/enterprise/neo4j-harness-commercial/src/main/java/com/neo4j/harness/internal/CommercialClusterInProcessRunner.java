/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness.internal;

import com.neo4j.server.security.enterprise.configuration.SecuritySettings;

import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.configuration.GraphDatabaseSettings;

import static org.neo4j.logging.FormattedLogProvider.toOutputStream;

public class CommercialClusterInProcessRunner
{
    public static void main( String[] args )
    {
        try
        {
            Path clusterPath = Files.createTempDirectory( "causal-cluster" );
            System.out.println( "clusterPath = " + clusterPath );

            CausalClusterInProcessBuilder.CausalCluster cluster =
                    CausalClusterInProcessBuilder.init()
                            .withBuilder( CommercialInProcessNeo4jBuilder::new )
                            .withCores( 3 )
                            .withReplicas( 3 )
                            .withLogger( toOutputStream( System.out ) )
                            .atPath( clusterPath )
                            .withConfig( GraphDatabaseSettings.auth_enabled.name(), "true" )
                            .withConfig( SecuritySettings.auth_provider.name(), SecuritySettings.SYSTEM_GRAPH_REALM_NAME )
                            .build();

            System.out.println( "Waiting for cluster to boot up..." );
            cluster.boot();

            System.out.println( "Press ENTER to exit..." );
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
