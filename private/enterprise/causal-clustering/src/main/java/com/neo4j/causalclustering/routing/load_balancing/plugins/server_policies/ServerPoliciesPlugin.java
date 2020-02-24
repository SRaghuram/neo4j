/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.DiscoveryServerInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingPlugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.values.virtual.MapValue;

import static java.util.Collections.emptyList;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * The server policies plugin defines policies on the server-side which
 * can be bound to by a client by supplying a appropriately formed context.
 *
 * An example would be to define different policies for different regions.
 */
@ServiceProvider
public class ServerPoliciesPlugin implements LoadBalancingPlugin
{
    public static final String PLUGIN_NAME = "server_policies";

    private AddressCollector addressCollector;
    private Policies policies;

    @Override
    public void validate( Config config, Log log )
    {
        FilteringPolicyLoader.loadServerPolicies( config, log );
    }

    @Override
    public void init( TopologyService topologyService, LeaderService leaderService, LogProvider logProvider, Config config )
    {
        var log = logProvider.getLog( getClass() );
        this.policies = FilteringPolicyLoader.loadServerPolicies( config, log );
        this.addressCollector = new AddressCollector( topologyService, leaderService, config, log );
    }

    @Override
    public String pluginName()
    {
        return PLUGIN_NAME;
    }

    @Override
    public boolean isShufflingPlugin()
    {
        return true;
    }

    @Override
    public RoutingResult run( NamedDatabaseId namedDatabaseId, MapValue context ) throws ProcedureException
    {
        var policy = policies.selectFor( context );
        return addressCollector.createRoutingResult( namedDatabaseId, policy );
    }
}
