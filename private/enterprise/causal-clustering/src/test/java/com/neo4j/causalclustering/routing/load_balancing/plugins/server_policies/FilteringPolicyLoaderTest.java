/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import com.neo4j.causalclustering.routing.load_balancing.filters.Filter;
import org.junit.Test;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.load_balancing_config;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.values.storable.Values.stringValue;

public class FilteringPolicyLoaderTest
{
    @Test
    public void shouldLoadConfiguredPolicies() throws Exception
    {
        // given
        String pluginName = "server_policies";

        Object[][] input = {
                {
                        "asia_west",

                        "groups(asia_west) -> min(2);" +
                        "groups(asia) -> min(2);",

                        FilterBuilder.filter().groups( "asia_west" ).min( 2 ).newRule()
                                .groups( "asia" ).min( 2 ).newRule()
                                .all() // implicit
                                .build()
                },
                {
                        "asia_east",

                        "groups(asia_east) -> min(2);" +
                        "groups(asia) -> min(2);",

                        FilterBuilder.filter().groups( "asia_east" ).min( 2 ).newRule()
                                .groups( "asia" ).min( 2 ).newRule()
                                .all() // implicit
                                .build()
                },
                {
                        "asia_only",

                        "groups(asia);" +
                        "halt();",

                        FilterBuilder.filter().groups( "asia" ).build()
                },
        };

        Config config = Config.defaults();

        for ( Object[] row : input )
        {
            String policyName = (String) row[0];
            String filterSpec = (String) row[1];
            config.augment( configNameFor( pluginName, policyName ), filterSpec );
        }

        // when
        Policies policies = FilteringPolicyLoader.load( config, pluginName, mock( Log.class ) );

        // then
        for ( Object[] row : input )
        {
            String policyName = (String) row[0];
            Policy loadedPolicy = policies.selectFor( policyNameContext( policyName ) );
            @SuppressWarnings( "unchecked" )
            Policy expectedPolicy = new FilteringPolicy( (Filter<ServerInfo>) row[2] );
            assertEquals( expectedPolicy, loadedPolicy );
        }
    }

    private static MapValue policyNameContext( String policyName )
    {
        return VirtualValues.map( new String[]{Policies.POLICY_KEY}, new AnyValue[]{stringValue( policyName )} );
    }

    private static String configNameFor( String pluginName, String policyName )
    {
        return format( "%s.%s.%s", load_balancing_config.name(), pluginName, policyName );
    }
}
