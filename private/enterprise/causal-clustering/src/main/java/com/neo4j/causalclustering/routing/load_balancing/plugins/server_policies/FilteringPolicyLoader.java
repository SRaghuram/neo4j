/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import com.neo4j.causalclustering.core.LoadBalancingServerPoliciesGroup;

import org.neo4j.configuration.Config;
import org.neo4j.logging.Log;

/**
 * Loads filters under the name space of a particular plugin.
 */
class FilteringPolicyLoader
{
    private FilteringPolicyLoader()
    {
    }

    static Policies loadServerPolicies( Config config, Log log )
    {
        Policies policies = new Policies( log );

        config.getGroups( LoadBalancingServerPoliciesGroup.class ).forEach( ( id, policy ) ->
        {
            policies.addPolicy( policy.name(), new FilteringPolicy( config.get( policy.value ) ) );
        } );

        return policies;
    }
}
