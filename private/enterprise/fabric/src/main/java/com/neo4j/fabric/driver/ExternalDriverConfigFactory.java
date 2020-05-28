/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.configuration.DriverApi;
import com.neo4j.configuration.FabricEnterpriseConfig;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.configuration.ssl.SslPolicyScope;
import org.neo4j.driver.Config;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.net.ServerAddress;
import org.neo4j.fabric.executor.Location;
import org.neo4j.ssl.config.SslPolicyLoader;

public class ExternalDriverConfigFactory extends AbstractDriverConfigFactory
{
    private final FabricEnterpriseConfig fabricConfig;
    private final Map<Long,FabricEnterpriseConfig.GraphDriverConfig> graphDriverConfigs;

    public ExternalDriverConfigFactory( FabricEnterpriseConfig fabricConfig, org.neo4j.configuration.Config serverConfig, SslPolicyLoader sslPolicyLoader )
    {
        super( serverConfig, sslPolicyLoader, SslPolicyScope.FABRIC );

        this.fabricConfig = fabricConfig;

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
        var builder = prebuildConfig( new PropertyExtractor()
        {
            @Override
            public <T> T extract( Function<FabricEnterpriseConfig.DriverConfig,T> driverConfigExtractor )
            {
                return getProperty( location, driverConfigExtractor );
            }
        } );

        var serverAddresses = location.getUri().getAddresses().stream()
                .map( address -> ServerAddress.of( address.getHostname(), address.getPort() ) )
                .collect( Collectors.toSet());

        return builder
                .withResolver( mainAddress -> serverAddresses )
                .build();
    }

    @Override
    public SecurityPlan createSecurityPlan( Location.Remote location )
    {
        var graphDriverConfig = graphDriverConfigs.get( location.getGraphId() );
        return createSecurityPlan( graphDriverConfig != null && !graphDriverConfig.isSslEnabled() );
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
}
