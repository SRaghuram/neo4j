/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.fabric.config.FabricEnterpriseConfig;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.function.Function;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import org.neo4j.configuration.ssl.SslPolicyScope;
import org.neo4j.driver.Config;
import org.neo4j.driver.Logging;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.logging.Level;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.ssl.config.SslPolicyLoader;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.OFF;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;
import static org.neo4j.configuration.GraphDatabaseSettings.store_internal_log_level;

public abstract class AbstractDriverConfigFactory
{
    private final Level serverLogLevel;
    private final SSLContext sslContext;
    private final SslPolicy sslPolicy;

    protected AbstractDriverConfigFactory( org.neo4j.configuration.Config serverConfig, SslPolicyLoader sslPolicyLoader, SslPolicyScope sslPolicyScope )
    {
        serverLogLevel = serverConfig.get( store_internal_log_level );

        if ( sslPolicyLoader.hasPolicyForSource( sslPolicyScope ) )
        {
            sslPolicy = sslPolicyLoader.getPolicy( sslPolicyScope );
            sslContext = createSslContext( sslPolicy );
        }
        else
        {
            sslPolicy = null;
            sslContext = null;
        }
    }

    private static SSLContext createSslContext( SslPolicy sslPolicy )
    {
        try
        {
            KeyManagerFactory keyManagerFactory = null;
            if ( sslPolicy.privateKey() != null && sslPolicy.certificateChain() != null )
            {
                KeyStore ks = KeyStore.getInstance( KeyStore.getDefaultType() );
                ks.load( null, null );
                // 'client-private-key' is an alias for the private key in the trust store.
                // Since there will be only one key in this key store, it does not matter how we call it
                ks.setKeyEntry( "client-private-key", sslPolicy.privateKey(), null, sslPolicy.certificateChain() );
                keyManagerFactory = KeyManagerFactory.getInstance( KeyManagerFactory.getDefaultAlgorithm() );
                keyManagerFactory.init( ks, null );
            }

            var trustManagerFactory = sslPolicy.getTrustManagerFactory();

            // 'TLS' means any supported version of TLS as opposed to requesting a concrete TLS version
            SSLContext ctx = SSLContext.getInstance( "TLS" );

            var keyManagers = keyManagerFactory == null ? null : keyManagerFactory.getKeyManagers();
            var trustManagers = trustManagerFactory == null ? null : trustManagerFactory.getTrustManagers();

            ctx.init( keyManagers, trustManagers, null );

            return ctx;
        }
        catch ( GeneralSecurityException | IOException e )
        {
            throw new IllegalArgumentException( "Failed to build SSL context", e );
        }
    }

    protected Config.ConfigBuilder prebuildConfig( PropertyExtractor propertyExtractor )
    {
        var builder = Config.builder();

        var logLeakedSessions = propertyExtractor.extract( FabricEnterpriseConfig.DriverConfig::getLogLeakedSessions );
        if ( logLeakedSessions )
        {
            builder.withLeakedSessionsLogging();
        }

        var idleTimeBeforeConnectionTest = propertyExtractor.extract( FabricEnterpriseConfig.DriverConfig::getIdleTimeBeforeConnectionTest );
        if ( idleTimeBeforeConnectionTest != null )
        {
            builder.withConnectionLivenessCheckTimeout( idleTimeBeforeConnectionTest.toMillis(), MILLISECONDS );
        }
        else
        {
            builder.withConnectionLivenessCheckTimeout( -1, MILLISECONDS );
        }

        var maxConnectionLifetime = propertyExtractor.extract( FabricEnterpriseConfig.DriverConfig::getMaxConnectionLifetime );
        if ( maxConnectionLifetime != null )
        {
            builder.withMaxConnectionLifetime( maxConnectionLifetime.toMillis(), MILLISECONDS );
        }

        var connectionAcquisitionTimeout = propertyExtractor.extract( FabricEnterpriseConfig.DriverConfig::getConnectionAcquisitionTimeout );
        if ( connectionAcquisitionTimeout != null )
        {
            builder.withConnectionAcquisitionTimeout( connectionAcquisitionTimeout.toMillis(), MILLISECONDS );
        }

        var connectTimeout = propertyExtractor.extract( FabricEnterpriseConfig.DriverConfig::getConnectTimeout );
        if ( connectTimeout != null )
        {
            builder.withConnectionTimeout( connectTimeout.toMillis(), MILLISECONDS );
        }

        var maxConnectionPoolSize = propertyExtractor.extract( FabricEnterpriseConfig.DriverConfig::getMaxConnectionPoolSize );
        if ( maxConnectionPoolSize != null )
        {
            builder.withMaxConnectionPoolSize( maxConnectionPoolSize );
        }

        return builder.withLogging( Logging.javaUtilLogging( getLoggingLevel( propertyExtractor ) ) );
    }

    protected SecurityPlan createSecurityPlan( boolean disableSsl )
    {
        if ( sslPolicy == null || disableSsl )
        {
            return new SecurityPlanImpl( false, null, false );
        }

        return new SecurityPlanImpl( true, sslContext, sslPolicy.isVerifyHostname() );
    }

    private java.util.logging.Level getLoggingLevel( PropertyExtractor propertyExtractor )
    {
        var loggingLevel = propertyExtractor.extract( FabricEnterpriseConfig.DriverConfig::getLoggingLevel );
        if ( loggingLevel == null )
        {
            loggingLevel = serverLogLevel;
        }

        switch ( loggingLevel )
        {
        case NONE:
            return OFF;
        case ERROR:
            return SEVERE;
        case WARN:
            return WARNING;
        case INFO:
            return java.util.logging.Level.INFO;
        case DEBUG:
            return FINE;
        default:
            throw new IllegalArgumentException( "Unexpected logging level: " + loggingLevel );
        }
    }

    protected interface PropertyExtractor
    {

        <T> T extract( Function<FabricEnterpriseConfig.DriverConfig,T> driverConfigExtractor );
    }

    private static final class SecurityPlanImpl implements SecurityPlan
    {

        private final boolean requiresEncryption;
        private final SSLContext sslContext;
        private final boolean requiresHostnameVerification;

        SecurityPlanImpl( boolean requiresEncryption, SSLContext sslContext, boolean requiresHostnameVerification )
        {
            this.requiresEncryption = requiresEncryption;
            this.sslContext = sslContext;
            this.requiresHostnameVerification = requiresHostnameVerification;
        }

        @Override
        public boolean requiresEncryption()
        {
            return requiresEncryption;
        }

        @Override
        public SSLContext sslContext()
        {
            return sslContext;
        }

        @Override
        public boolean requiresHostnameVerification()
        {
            return requiresHostnameVerification;
        }
    }
}
