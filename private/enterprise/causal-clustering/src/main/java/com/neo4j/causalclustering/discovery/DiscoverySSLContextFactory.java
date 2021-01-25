/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import org.neo4j.ssl.SslPolicy;

public class DiscoverySSLContextFactory
{
    public static final String PROTOCOL = "TLS";
    private final SslPolicy sslPolicy;

    public DiscoverySSLContextFactory( SslPolicy sslPolicy )
    {
        this.sslPolicy = sslPolicy;
    }

    public SSLContext sslContext()
    {
        try ( SecurePassword securePassword = new SecurePassword( 32, new SecureRandom() ) )
        {
            char[] password = securePassword.password();
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance( KeyManagerFactory.getDefaultAlgorithm() );
            KeyStore keyStore = sslPolicy.getKeyStore( password, password );
            keyManagerFactory.init( keyStore, password );

            SSLContext sslContext = SSLContext.getInstance( PROTOCOL );
            KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();
            TrustManager[] trustManagers = sslPolicy.getTrustManagerFactory().getTrustManagers();
            sslContext.init( keyManagers, trustManagers, null );

            return sslContext;
        }
        catch ( NoSuchAlgorithmException | KeyManagementException | UnrecoverableKeyException | KeyStoreException e )
        {
            throw new RuntimeException( "Error creating SSL context", e );
        }

    }
}
