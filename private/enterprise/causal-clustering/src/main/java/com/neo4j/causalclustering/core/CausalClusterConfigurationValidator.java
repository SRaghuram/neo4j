/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingPluginLoader;
import com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings;
import com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings.Mode;

import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GroupSettingValidator;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.graphdb.config.Setting;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.minimum_core_cluster_size_at_formation;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.minimum_core_cluster_size_at_runtime;

public class CausalClusterConfigurationValidator implements GroupSettingValidator
{
    @Override
    public String getPrefix()
    {
        return "causal_clustering";
    }

    @Override
    public String getDescription()
    {
        return "Validates causal clustering settings";
    }

    @Override
    public void validate( Map<Setting<?>,Object> values, Config config )
    {
        Mode mode = config.get( CommercialEditionSettings.mode );
        if ( mode.equals( Mode.CORE ) || mode.equals( Mode.READ_REPLICA ) )
        {
            validateInitialDiscoveryMembers( config );
            validateBoltConnector( config );
            LoadBalancingPluginLoader.validate( config, null );
            validateDeclaredClusterSizes( config );
        }
    }

    private void validateDeclaredClusterSizes( Config config )
    {
        int startup = config.get( minimum_core_cluster_size_at_formation );
        int runtime = config.get( minimum_core_cluster_size_at_runtime );

        if ( runtime > startup )
        {
            throw new IllegalArgumentException( String.format( "'%s' must be set greater than or equal to '%s'",
                    minimum_core_cluster_size_at_formation.name(), minimum_core_cluster_size_at_runtime.name() ) );
        }
    }

    private void validateBoltConnector( Config config )
    {
        if ( !config.get( BoltConnector.enabled ) )
        {
            throw new IllegalArgumentException( "A Bolt connector must be configured to run a cluster" );
        }
    }

    private void validateInitialDiscoveryMembers( Config config )
    {
        DiscoveryType discoveryType = config.get( CausalClusteringSettings.discovery_type );
        discoveryType.requiredSettings().forEach( setting -> {
            if ( !config.isExplicitlySet( setting ) )
            {
                throw new IllegalArgumentException( String.format( "Missing value for '%s', which is mandatory with '%s=%s'",
                        setting.name(), CausalClusteringSettings.discovery_type.name(), discoveryType ) );
            }
        } );
    }
}
