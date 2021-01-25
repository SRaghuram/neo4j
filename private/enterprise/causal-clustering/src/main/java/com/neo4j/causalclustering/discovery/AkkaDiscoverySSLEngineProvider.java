/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import akka.remote.artery.tcp.SSLEngineProvider;
import scala.Option;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

import org.neo4j.configuration.ssl.ClientAuth;
import org.neo4j.ssl.ClientSideHostnameVerificationEngineModification;
import org.neo4j.ssl.EssentialEngineModifications;
import org.neo4j.ssl.SslPolicy;

public class AkkaDiscoverySSLEngineProvider implements SSLEngineProvider
{
    private final SslPolicy sslPolicy;
    private final SSLContext sslContext;

    public AkkaDiscoverySSLEngineProvider( SslPolicy sslPolicy )
    {
        this.sslPolicy = sslPolicy;

        this.sslContext = new DiscoverySSLContextFactory( this.sslPolicy ).sslContext();
    }

    @Override
    public SSLEngine createServerSSLEngine( String hostname, int port )
    {
        SSLEngine sslEngine = createSSLEngine( false, hostname, port );
        sslEngine.setWantClientAuth( ClientAuth.OPTIONAL.equals( sslPolicy.getClientAuth() ) );
        sslEngine.setNeedClientAuth( ClientAuth.REQUIRE.equals( sslPolicy.getClientAuth() ) );
        return  sslEngine;
    }

    @Override
    public SSLEngine createClientSSLEngine( String hostname, int port )
    {
        SSLEngine sslEngine = createSSLEngine( true, hostname, port );
        if ( sslPolicy.isVerifyHostname() )
        {
            sslEngine = new ClientSideHostnameVerificationEngineModification().apply( sslEngine );
        }
        return sslEngine;
    }

    private SSLEngine createSSLEngine( boolean isClient, String hostname, int port )
    {
        SSLEngine sslEngine = sslContext.createSSLEngine( hostname, port );

        sslEngine = new EssentialEngineModifications( sslPolicy.getTlsVersions(), isClient ).apply( sslEngine );

        if ( sslPolicy.getCipherSuites() != null )
        {
            sslEngine.setEnabledCipherSuites( sslPolicy.getCipherSuites().toArray( new String[0] ) );
        }

        return sslEngine;
    }

    @Override
    public Option<Throwable> verifyClientSession( String hostname, SSLSession session )
    {
        return scala.Option.apply( null );
    }

    @Override
    public Option<Throwable> verifyServerSession( String hostname, SSLSession session )
    {
        return scala.Option.apply( null );
    }
}
