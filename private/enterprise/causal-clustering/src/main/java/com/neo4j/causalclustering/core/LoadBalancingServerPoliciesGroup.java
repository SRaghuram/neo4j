/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.routing.load_balancing.filters.Filter;
import com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.FilterConfigParser;
import com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.InvalidFilterSpecification;
import com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.ServerInfo;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.SettingValueParser;
import org.neo4j.graphdb.config.Setting;

@ServiceProvider
public final class LoadBalancingServerPoliciesGroup extends LoadBalancingPluginGroup
{
    private static final SettingValueParser<Filter<ServerInfo>> PARSER = new SettingValueParser<>()
    {
        @Override
        public Filter<ServerInfo> parse( String value )
        {
            try
            {
                return FilterConfigParser.parse( value );
            }
            catch ( InvalidFilterSpecification e )
            {
               throw new IllegalArgumentException( "Not a valid filter", e );
            }
        }

        @Override
        public String getDescription()
        {
            return "a load-balancing filter";
        }

        @Override
        public Class<Filter<ServerInfo>> getType()
        {
            //noinspection unchecked
            return (Class<Filter<ServerInfo>>) (Class) Filter.class;
        }
    };

    public final Setting<Filter<ServerInfo>> value = getBuilder( PARSER, null ).build();

    public static LoadBalancingServerPoliciesGroup group( String name )
    {
        return new LoadBalancingServerPoliciesGroup( name );
    }

    public LoadBalancingServerPoliciesGroup()
    {
        super( null, null ); // For ServiceLoader
    }

    private LoadBalancingServerPoliciesGroup( String name )
    {
        super( name, "server_policies" );
    }
}
