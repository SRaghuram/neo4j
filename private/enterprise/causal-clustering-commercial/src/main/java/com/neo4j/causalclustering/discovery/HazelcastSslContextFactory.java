/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.hazelcast.nio.ssl.SSLContextFactory;

import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.util.Properties;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.ssl.SslPolicy;

import static java.lang.String.format;

class HazelcastSslContextFactory implements SSLContextFactory
{
    private static final String PROTOCOL = "TLS";

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
        SSLContext sslCtx;

        try
        {
            sslCtx = SSLContext.getInstance( PROTOCOL );
        }
        catch ( NoSuchAlgorithmException e )
        {
            throw new RuntimeException( e );
        }

        KeyManagerFactory keyManagerFactory;
        try
        {
            keyManagerFactory = KeyManagerFactory.getInstance( KeyManagerFactory.getDefaultAlgorithm() );
        }
        catch ( NoSuchAlgorithmException e )
        {
            throw new RuntimeException( e );
        }

        SecureRandom rand = new SecureRandom();
        char[] password = new char[32];
        for ( int i = 0; i < password.length; i++ )
        {
            password[i] = (char) rand.nextInt( Character.MAX_VALUE + 1 );
        }

        try
        {
            KeyStore keyStore = sslPolicy.getKeyStore( password, password );
            keyManagerFactory.init( keyStore, password );
        }
        catch ( KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            for ( int i = 0; i < password.length; i++ )
            {
                password[i] = 0;
            }
        }

        KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();
        TrustManager[] trustManagers = sslPolicy.getTrustManagerFactory().getTrustManagers();

        try
        {
            sslCtx.init( keyManagers, trustManagers, null );
        }
        catch ( KeyManagementException e )
        {
            throw new RuntimeException( e );
        }

        if ( sslPolicy.getTlsVersions() != null )
        {
            log.warn( format( "Restricting TLS versions through policy not supported." +
                    " System defaults for %s family will be used.", PROTOCOL ) );
        }

        if ( sslPolicy.getCipherSuites() != null )
        {
            log.warn( "Restricting ciphers through policy not supported." +
                    " System defaults will be used." );
        }

        return sslCtx;
    }
}
