/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SSLConfig;

import org.neo4j.logging.LogProvider;
import org.neo4j.ssl.SslPolicy;

// TODO: Investigate if client auth actually can be configured as below.
// TODO: Fix the enterprise check or fix HZ to not silently fail otherwise.
public class HazelcastSslConfiguration
{
    public static void configureSsl( NetworkConfig networkConfig, SslPolicy sslPolicy, LogProvider logProvider )
    {
        SSLConfig sslConfig = commonSslConfig( sslPolicy, logProvider );
        networkConfig.setSSLConfig( sslConfig );
    }

    public static void configureSsl( ClientNetworkConfig clientNetworkConfig, SslPolicy sslPolicy, LogProvider logProvider )
    {
        SSLConfig sslConfig = commonSslConfig( sslPolicy, logProvider );
        clientNetworkConfig.setSSLConfig( sslConfig );
    }

    private static SSLConfig commonSslConfig( SslPolicy sslPolicy, LogProvider logProvider )
    {
        SSLConfig sslConfig = new SSLConfig();

        if ( sslPolicy == null )
        {
            return sslConfig;
        }

        /* N.B: this check is currently broken in HZ 3.7 - SSL might be silently unavailable */
//        if ( !com.hazelcast.instance.BuildInfoProvider.getBuildInfo().isEnterprise() )
//        {
//            throw new UnsupportedOperationException( "Hazelcast can only use SSL under the enterprise version." );
//        }

        sslConfig.setFactoryImplementation(
                new HazelcastSslContextFactory( sslPolicy, logProvider ) )
                .setEnabled( true );

        switch ( sslPolicy.getClientAuth() )
        {
        case REQUIRE:
            sslConfig.setProperty( "javax.net.ssl.mutualAuthentication", "REQUIRED" );
            break;
        case OPTIONAL:
            sslConfig.setProperty( "javax.net.ssl.mutualAuthentication", "OPTIONAL" );
            break;
        case NONE:
            break;
        default:
            throw new IllegalArgumentException( "Not supported: " + sslPolicy.getClientAuth() );
        }

        return sslConfig;
    }
}
