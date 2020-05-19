/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.fabric.config.FabricEnterpriseConfig;

import java.util.function.Function;

import org.neo4j.configuration.ssl.SslPolicyScope;
import org.neo4j.driver.Config;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.fabric.executor.Location;
import org.neo4j.ssl.config.SslPolicyLoader;

public class ClusterDriverConfigFactory extends AbstractDriverConfigFactory
{
    private FabricEnterpriseConfig fabricConfig;

    public ClusterDriverConfigFactory( FabricEnterpriseConfig fabricConfig, org.neo4j.configuration.Config serverConfig, SslPolicyLoader sslPolicyLoader )
    {
        super( serverConfig, sslPolicyLoader, SslPolicyScope.CLUSTER );

        this.fabricConfig = fabricConfig;
    }

    @Override
    public Config createConfig( Location.Remote location )
    {
        var configBuilder = prebuildConfig( new PropertyExtractor()
        {
            @Override
            public <T> T extract( Function<FabricEnterpriseConfig.DriverConfig,T> driverConfigExtractor )
            {
                return driverConfigExtractor.apply( fabricConfig.getGlobalDriverConfig().getDriverConfig() );
            }
        } );

        return configBuilder.build();
    }

    @Override
    public SecurityPlan createSecurityPlan( Location.Remote location )
    {
        return createSecurityPlan( false );
    }

    @Override
    public DriverApi getDriverApi( Location.Remote location )
    {
        return fabricConfig.getGlobalDriverConfig().getDriverConfig().getDriverApi();
    }
}
