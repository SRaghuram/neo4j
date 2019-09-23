/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.fabric.config.FabricConfig;

import java.util.function.Function;

import org.neo4j.driver.Config;
import org.neo4j.driver.Logging;
import org.neo4j.logging.Level;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.OFF;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;
import static org.neo4j.configuration.GraphDatabaseSettings.store_internal_log_level;

class DriverConfigFactory
{
    private final FabricConfig fabricConfig;
    private final Level serverLogLevel;

    DriverConfigFactory( FabricConfig fabricConfig, org.neo4j.configuration.Config serverConfig )
    {
        this.fabricConfig = fabricConfig;

        serverLogLevel = serverConfig.get( store_internal_log_level );
    }

    Config createConfig( FabricConfig.Graph graph )
    {
        var builder = Config.builder();

        var logLeakedSessions = getProperty( graph, FabricConfig.DriverConfig::getLogLeakedSessions );
        if ( logLeakedSessions != null && logLeakedSessions )
        {
            builder.withLeakedSessionsLogging();
        }

        var metricsEnabled = getProperty( graph, FabricConfig.DriverConfig::getMetricsEnabled );
        if ( metricsEnabled != null )
        {
            if ( metricsEnabled )
            {
                builder.withDriverMetrics();
            }
            else
            {
                builder.withoutDriverMetrics();
            }
        }

        var encrypted = getProperty( graph, FabricConfig.DriverConfig::getEncrypted );
        if ( encrypted != null )
        {
            if ( encrypted )
            {
                builder.withEncryption();
            }
            else
            {
                builder.withoutEncryption();
            }
        }

        var trustStrategy = getTrustStrategy( graph );
        if ( trustStrategy != null )
        {
            builder.withTrustStrategy( trustStrategy );
        }

        var idleTimeBeforeConnectionTest = getProperty( graph, FabricConfig.DriverConfig::getIdleTimeBeforeConnectionTest );
        if ( idleTimeBeforeConnectionTest != null )
        {
            builder.withConnectionLivenessCheckTimeout( idleTimeBeforeConnectionTest.toMillis(), MILLISECONDS );
        }

        var maxConnectionLifetime = getProperty( graph, FabricConfig.DriverConfig::getMaxConnectionLifetime );
        if ( maxConnectionLifetime != null )
        {
            builder.withMaxConnectionLifetime( maxConnectionLifetime.toMillis(), MILLISECONDS );
        }

        var connectionAcquisitionTimeout = getProperty( graph, FabricConfig.DriverConfig::getConnectionAcquisitionTimeout );
        if ( connectionAcquisitionTimeout != null )
        {
            builder.withConnectionAcquisitionTimeout( connectionAcquisitionTimeout.toMillis(), MILLISECONDS );
        }

        var connectTimeout = getProperty( graph, FabricConfig.DriverConfig::getConnectTimeout );
        if ( connectTimeout != null )
        {
            builder.withConnectionTimeout( connectTimeout.toMillis(), MILLISECONDS );
        }

        var retryMaxTime = getProperty( graph, FabricConfig.DriverConfig::getRetryMaxTime );
        if ( retryMaxTime != null )
        {
            builder.withMaxTransactionRetryTime( retryMaxTime.toMillis(), MILLISECONDS );
        }

        var loadBalancingStrategy = getLoadBalancingStrategy( graph );
        if ( loadBalancingStrategy != null )
        {
            builder.withLoadBalancingStrategy( loadBalancingStrategy );
        }

        var maxConnectionPoolSize = getProperty( graph, FabricConfig.DriverConfig::getMaxConnectionPoolSize );
        if ( maxConnectionPoolSize != null )
        {
            builder.withMaxConnectionPoolSize( maxConnectionPoolSize );
        }

        return builder.withLogging( Logging.javaUtilLogging( getLoggingLevel( graph ) ) )
                .build();
    }

    <T> T getProperty( FabricConfig.Graph graph, Function<FabricConfig.DriverConfig,T> extractor )
    {
        var graphDriverConfig = graph.getDriverConfig();

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

    private java.util.logging.Level getLoggingLevel( FabricConfig.Graph graph )
    {
        var loggingLevel = getProperty( graph, FabricConfig.DriverConfig::getLoggingLevel );
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

    private Config.TrustStrategy getTrustStrategy( FabricConfig.Graph graph )
    {
        var trustStrategy = getProperty( graph, FabricConfig.DriverConfig::getTrustStrategy );

        if ( trustStrategy == null )
        {
            return null;
        }

        switch ( trustStrategy )
        {
        case TRUST_SYSTEM_CA_SIGNED_CERTIFICATES:
            return Config.TrustStrategy.trustSystemCertificates();
        case TRUST_ALL_CERTIFICATES:
            return Config.TrustStrategy.trustAllCertificates();
        default:
            throw new IllegalArgumentException( "Unexpected trust strategy: " + trustStrategy );
        }
    }

    private Config.LoadBalancingStrategy getLoadBalancingStrategy( FabricConfig.Graph graph )
    {
        var loadBalancingStrategy = getProperty( graph, FabricConfig.DriverConfig::getLoadBalancingStrategy );
        if ( loadBalancingStrategy == null )
        {
            return null;
        }

        switch ( loadBalancingStrategy )
        {
        case ROUND_ROBIN:
            return Config.LoadBalancingStrategy.ROUND_ROBIN;
        case LEAST_CONNECTED:
            return Config.LoadBalancingStrategy.LEAST_CONNECTED;
        default:
            throw new IllegalArgumentException( "Unexpected load balancing strategy: " + loadBalancingStrategy );
        }
    }
}
