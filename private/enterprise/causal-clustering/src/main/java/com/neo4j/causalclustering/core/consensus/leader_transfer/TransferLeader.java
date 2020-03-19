/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.RaftMessages.LeadershipTransfer.Proposal;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.Inbound;
import com.neo4j.dbms.database.ClusteredDatabaseContext;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.NamedDatabaseId;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

class TransferLeader implements Runnable
{
    public static final RandomStrategy PRIORITISED_SELECTION_STRATEGY = new RandomStrategy();
    private final TopologyService topologyService;
    private final Config config;
    private DatabaseManager<ClusteredDatabaseContext> databaseManager;
    private Inbound.MessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>> messageHandler;
    private MemberId myself;
    private DatabasePenalties databasePenalties;
    private SelectionStrategy selectionStrategy;

    TransferLeader( TopologyService topologyService, Config config, DatabaseManager<ClusteredDatabaseContext> databaseManager,
            Inbound.MessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>> messageHandler, MemberId myself,
            DatabasePenalties databasePenalties, SelectionStrategy leaderLoadBalancing )
    {
        this.topologyService = topologyService;
        this.config = config;
        this.databaseManager = databaseManager;
        this.messageHandler = messageHandler;
        this.myself = myself;
        this.databasePenalties = databasePenalties;
        this.selectionStrategy = leaderLoadBalancing;
    }

    @Override
    public void run()
    {
        databasePenalties.clean();

        var leaderTransferContext = createContext( notPrioritisedLeadership(), PRIORITISED_SELECTION_STRATEGY );

        if ( leaderTransferContext == null )
        {
            leaderTransferContext = createContext( myLeaderships(), selectionStrategy );
        }

        if ( leaderTransferContext != null )
        {
            handleProposal( leaderTransferContext );
        }
    }

    private LeaderTransferContext createContext( List<NamedDatabaseId> databaseIds, SelectionStrategy selectionStrategy )
    {
        if ( !databaseIds.isEmpty() )
        {
            var validTopologies = databaseIds.stream().map( topologyService::coreTopologyForDatabase )
                    .map( ct -> new TopologyContext( ct.databaseId(), ct.raftId(), ct.members().keySet()
                            .stream().filter( member -> databasePenalties.notSuspended( ct.databaseId(), member ) && !member.equals( myself ) )
                            .collect( toSet() ) ) ).collect( toList() );
            return selectionStrategy.select( validTopologies );
        }
        return null;
    }

    private void handleProposal( LeaderTransferContext transferContext )
    {
        messageHandler.handle( RaftMessages.ReceivedInstantRaftIdAwareMessage.of( Instant.now(),
                                                                                  transferContext.raftId(), new Proposal( myself, transferContext.to(),
                                                                                                                          getPrioritisedGroups( config ) ) ) );
    }

    private List<NamedDatabaseId> notPrioritisedLeadership()
    {
        // TODO:  Prioritized should be per database
        Set<String> myGroups = getMyGroups( config );
        Set<String> myPrioritizedGroups = getPrioritisedGroups( config );
        myPrioritizedGroups.retainAll( myGroups );
        if ( !myPrioritizedGroups.isEmpty() )
        {
            return List.of();
        }
        return myLeaderships();
    }

    private List<NamedDatabaseId> myLeaderships()
    {
        return databaseManager.registeredDatabases().values().stream().filter( this::amLeader ).map( ClusteredDatabaseContext::databaseId )
                .collect( toList() );
    }

    private boolean amLeader( ClusteredDatabaseContext context )
    {
        return context.leaderLocator().map( leaderLocator ->
                                            {
                                                var leader = leaderLocator.getLeader();
                                                return leader != null && leader.equals( myself );
                                            } ).orElse( false );
    }

    private Set<String> getMyGroups( Config config )
    {
        return new HashSet<>( config.get( CausalClusteringSettings.server_groups ) );
    }

    private Set<String> getPrioritisedGroups( Config config )
    {
        return new HashSet<>( config.get( CausalClusteringSettings.leadership_priority_groups ) );
    }
}
