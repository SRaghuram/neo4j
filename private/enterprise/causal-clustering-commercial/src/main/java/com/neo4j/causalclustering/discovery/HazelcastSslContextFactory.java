/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.hazelcast.nio.ssl.SSLContextFactory;

import java.util.Properties;
import javax.net.ssl.SSLContext;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.ssl.SslPolicy;

import static java.lang.String.format;

class HazelcastSslContextFactory implements SSLContextFactory
{
    private final SslPolicy sslPolicy;
    private final Log log;

    HazelcastSslContextFactory( SslPolicy sslPolicy, LogProvider logProvider )
    {
        this.sslPolicy = sslPolicy;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void init( Properties properties )
    {
    }

    @Override
    public SSLContext getSSLContext()
    {
        if ( sslPolicy.getTlsVersions() != null )
        {
            log.warn( format( "Restricting TLS versions through policy not supported." +
                    " System defaults for %s family will be used.", DiscoverySSLContextFactory.PROTOCOL ) );
        }

        if ( sslPolicy.getCipherSuites() != null )
        {
            log.warn( "Restricting ciphers through policy not supported." +
                    " System defaults will be used." );
        }

        return new DiscoverySSLContextFactory( sslPolicy ).sslContext();
    }
}
