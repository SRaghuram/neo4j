/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.ssl;

import io.netty.handler.ssl.SslProvider;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.ssl.PemSslPolicyConfig;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.ssl.SslResource;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.ssl.config.SslSystemSettings;

public class SslContextFactory
{
    public interface Ciphers
    {
        SslParameters ciphers( String... ciphers );
    }

    public static class SslParameters implements Ciphers
    {
        private List<String> protocols;
        private List<String> ciphers;

        private SslParameters( List<String> protocols, List<String> ciphers )
        {
            this.protocols = protocols;
            this.ciphers = ciphers;
        }

        public static Ciphers protocols( String... protocols )
        {
            return new SslParameters( listOrNull( protocols ), null );
        }

        @Override
        public SslParameters ciphers( String... ciphers )
        {
            this.ciphers = listOrNull( ciphers );
            return this;
        }

        /**
         * The low-level frameworks use null to signify that defaults shall be used, and so does our SSL framework.
         */
        private static List<String> listOrNull( String[] parts )
        {
            return parts.length > 0 ? Arrays.asList( parts ) : null;
        }

        @Override
        public String toString()
        {
            return "SslParameters{" + "protocols='" + protocols + '\'' + ", ciphers='" + ciphers + '\'' + '}';
        }
    }

    public static SslPolicy makeSslPolicy( SslResource sslResource, SslParameters params )
    {
        return makeSslPolicy( sslResource, SslProvider.JDK, params.protocols, params.ciphers );
    }

    public static SslPolicy makeSslPolicy( SslResource sslResource, SslProvider sslProvider )
    {
        return makeSslPolicy( sslResource, sslProvider, null, null );
    }

    public static SslPolicy makeSslPolicy( SslResource sslResource )
    {
        return makeSslPolicy( sslResource, SslProvider.JDK, null, null );
    }

    public static SslPolicy makeSslPolicy( SslResource sslResource, SslProvider sslProvider, List<String> protocols, List<String> ciphers )
    {
        Config.Builder config = Config.newBuilder();
        config.set( SslSystemSettings.netty_ssl_provider, sslProvider );

        PemSslPolicyConfig policyConfig = PemSslPolicyConfig.group( "default" );
        File baseDirectory = sslResource.privateKey().getParentFile();
        new File( baseDirectory, "trusted" ).mkdirs();
        new File( baseDirectory, "revoked" ).mkdirs();

        config.set( policyConfig.base_directory, baseDirectory.toPath() );
        config.set( policyConfig.private_key, sslResource.privateKey().toPath() );
        config.set( policyConfig.public_certificate, sslResource.publicCertificate().toPath() );
        config.set( policyConfig.trusted_dir, sslResource.trustedDirectory().toPath() );
        config.set( policyConfig.revoked_dir, sslResource.revokedDirectory().toPath() );
        config.set( policyConfig.verify_hostname, false );

        if ( protocols != null )
        {
            config.set( policyConfig.tls_versions, protocols );
        }

        if ( ciphers != null )
        {
            config.set( policyConfig.ciphers, ciphers );
        }

        SslPolicyLoader sslPolicyFactory =
                SslPolicyLoader.create( config.build(), NullLogProvider.getInstance() );

        return sslPolicyFactory.getPolicy( "default" );
    }
}
