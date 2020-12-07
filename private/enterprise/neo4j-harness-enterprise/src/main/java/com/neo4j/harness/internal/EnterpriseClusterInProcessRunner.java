/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness.internal;

import com.neo4j.configuration.SecuritySettings;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.logging.log4j.Log4jLogProvider;

public class EnterpriseClusterInProcessRunner
{
    public static void main( String[] args )
    {
        try
        {
            Path clusterPath = Files.createTempDirectory( "causal-cluster" );
            System.out.println( "clusterPath = " + clusterPath );

            CausalClusterInProcessBuilder.CausalCluster cluster =
                    CausalClusterInProcessBuilder.init()
                            .withBuilder( EnterpriseInProcessNeo4jBuilder::new )
                            .withCores( 3 )
                            .withReplicas( 0 )
                            .withLogger( new Log4jLogProvider( System.out ) )
                            .atPath( clusterPath )
                            .withConfig( GraphDatabaseSettings.auth_enabled, false )
                            .withConfig( SecuritySettings.authentication_providers,  List.of( SecuritySettings.NATIVE_REALM_NAME ) )
                            .withConfig( SecuritySettings.authorization_providers, List.of( SecuritySettings.NATIVE_REALM_NAME ) )
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
