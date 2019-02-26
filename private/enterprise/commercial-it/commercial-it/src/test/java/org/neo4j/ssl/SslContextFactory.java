/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.ssl;

import io.netty.handler.ssl.SslProvider;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.logging.NullLogProvider;
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
        private String protocols;
        private String ciphers;

        private SslParameters( String protocols, String ciphers )
        {
            this.protocols = protocols;
            this.ciphers = ciphers;
        }

        public static Ciphers protocols( String... protocols )
        {
            return new SslParameters( joinOrNull( protocols ), null );
        }

        @Override
        public SslParameters ciphers( String... ciphers )
        {
            this.ciphers = joinOrNull( ciphers );
            return this;
        }

        /**
         * The low-level frameworks use null to signify that defaults shall be used, and so does our SSL framework.
         */
        private static String joinOrNull( String[] parts )
        {
            return parts.length > 0 ? String.join( ",", parts ) : null;
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

    public static SslPolicy makeSslPolicy( SslResource sslResource, SslProvider sslProvider, String protocols, String ciphers )
    {
        Map<String,String> config = new HashMap<>();
        config.put( SslSystemSettings.netty_ssl_provider.name(), sslProvider.name() );

        SslPolicyConfig policyConfig = new SslPolicyConfig( "default" );
        File baseDirectory = sslResource.privateKey().getParentFile();
        new File( baseDirectory, "trusted" ).mkdirs();
        new File( baseDirectory, "revoked" ).mkdirs();

        config.put( policyConfig.base_directory.name(), baseDirectory.getPath() );
        config.put( policyConfig.private_key.name(), sslResource.privateKey().getPath() );
        config.put( policyConfig.public_certificate.name(), sslResource.publicCertificate().getPath() );
        config.put( policyConfig.trusted_dir.name(), sslResource.trustedDirectory().getPath() );
        config.put( policyConfig.revoked_dir.name(), sslResource.revokedDirectory().getPath() );
        config.put( policyConfig.verify_hostname.name(), "false" );

        if ( protocols != null )
        {
            config.put( policyConfig.tls_versions.name(), protocols );
        }

        if ( ciphers != null )
        {
            config.put( policyConfig.ciphers.name(), ciphers );
        }

        SslPolicyLoader sslPolicyFactory =
                SslPolicyLoader.create( Config.fromSettings( config ).build(), NullLogProvider.getInstance() );

        return sslPolicyFactory.getPolicy( "default" );
    }
}
