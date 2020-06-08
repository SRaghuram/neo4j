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
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import org.neo4j.configuration.ssl.SslPolicyScope;
import org.neo4j.driver.Config;
import org.neo4j.driver.Logging;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.net.ServerAddress;
import org.neo4j.fabric.executor.Location;
import org.neo4j.logging.Level;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.ssl.config.SslPolicyLoader;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.OFF;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;
import static org.neo4j.configuration.GraphDatabaseSettings.store_internal_log_level;

public class ExternalDriverConfigFactory implements DriverConfigFactory
{
    private final FabricEnterpriseConfig fabricConfig;
    private final Level serverLogLevel;
    private final SSLContext sslContext;
    private final SslPolicy sslPolicy;
    private final Map<Long,FabricEnterpriseConfig.GraphDriverConfig> graphDriverConfigs;

    public ExternalDriverConfigFactory( FabricEnterpriseConfig fabricConfig, org.neo4j.configuration.Config serverConfig, SslPolicyLoader sslPolicyLoader )
    {
        this.fabricConfig = fabricConfig;

        serverLogLevel = serverConfig.get( store_internal_log_level );

        if ( sslPolicyLoader.hasPolicyForSource( SslPolicyScope.FABRIC ) )
        {
            sslPolicy = sslPolicyLoader.getPolicy( SslPolicyScope.FABRIC );
            sslContext = createSslContext( sslPolicy );
        }
        else
        {
            sslPolicy = null;
            sslContext = null;
        }

        if ( fabricConfig.getDatabase() != null )
        {
            graphDriverConfigs = fabricConfig.getDatabase().getGraphs().stream()
                    .filter( graph -> graph.getDriverConfig() != null )
                    .collect( Collectors.toMap( FabricEnterpriseConfig.Graph::getId, FabricEnterpriseConfig.Graph::getDriverConfig ) );
        }
        else
        {
            graphDriverConfigs = Map.of();
        }
    }

    @Override
    public Config createConfig( Location.Remote location )
    {
        var builder = Config.builder();

        var logLeakedSessions = getProperty( location, FabricEnterpriseConfig.DriverConfig::getLogLeakedSessions );
        if ( logLeakedSessions )
        {
            builder.withLeakedSessionsLogging();
        }

        var idleTimeBeforeConnectionTest = getProperty( location, FabricEnterpriseConfig.DriverConfig::getIdleTimeBeforeConnectionTest );
        if ( idleTimeBeforeConnectionTest != null )
        {
            builder.withConnectionLivenessCheckTimeout( idleTimeBeforeConnectionTest.toMillis(), MILLISECONDS );
        }
        else
        {
            builder.withConnectionLivenessCheckTimeout( -1, MILLISECONDS );
        }

        var maxConnectionLifetime = getProperty( location, FabricEnterpriseConfig.DriverConfig::getMaxConnectionLifetime );
        if ( maxConnectionLifetime != null )
        {
            builder.withMaxConnectionLifetime( maxConnectionLifetime.toMillis(), MILLISECONDS );
        }

        var connectionAcquisitionTimeout = getProperty( location, FabricEnterpriseConfig.DriverConfig::getConnectionAcquisitionTimeout );
        if ( connectionAcquisitionTimeout != null )
        {
            builder.withConnectionAcquisitionTimeout( connectionAcquisitionTimeout.toMillis(), MILLISECONDS );
        }

        var connectTimeout = getProperty( location, FabricEnterpriseConfig.DriverConfig::getConnectTimeout );
        if ( connectTimeout != null )
        {
            builder.withConnectionTimeout( connectTimeout.toMillis(), MILLISECONDS );
        }

        var maxConnectionPoolSize = getProperty( location, FabricEnterpriseConfig.DriverConfig::getMaxConnectionPoolSize );
        if ( maxConnectionPoolSize != null )
        {
            builder.withMaxConnectionPoolSize( maxConnectionPoolSize );
        }

        var serverAddresses = location.getUri().getAddresses().stream()
                .map( address -> ServerAddress.of( address.getHostname(), address.getPort() ) )
                .collect( Collectors.toSet());

        return builder
                .withResolver( mainAddress -> serverAddresses )
                .withLogging( Logging.javaUtilLogging( getLoggingLevel( location ) ) ).build();
    }

    @Override
    public SecurityPlan createSecurityPlan( Location.Remote location )
    {
        var graphDriverConfig = graphDriverConfigs.get( location.getGraphId() );

        if ( sslPolicy == null || (graphDriverConfig != null && !graphDriverConfig.isSslEnabled()) )
        {
            return new SecurityPlanImpl( false, null, false );
        }

        return new SecurityPlanImpl( true, sslContext, sslPolicy.isVerifyHostname() );
    }

    @Override
    public DriverApi getDriverApi( Location.Remote location )
    {
        return getProperty( location, FabricEnterpriseConfig.DriverConfig::getDriverApi );
    }

    private <T> T getProperty( Location.Remote location, Function<FabricEnterpriseConfig.DriverConfig,T> extractor )
    {
        var graphDriverConfig = graphDriverConfigs.get( location.getGraphId() );

        if ( graphDriverConfig != null )
        {
            // this means that graph-specific driver configuration exists and it can override
            // some properties of global driver configuration
            var configValue = extractor.apply( graphDriverConfig );
            if ( configValue != null )
            {
                return configValue;
            }
        }

        return extractor.apply( fabricConfig.getGlobalDriverConfig().getDriverConfig() );
    }

    private java.util.logging.Level getLoggingLevel( Location.Remote location )
    {
        var loggingLevel = getProperty( location, FabricEnterpriseConfig.DriverConfig::getLoggingLevel );
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
                // Since there will be only one key in this truststore, it does not matter how we call it
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
