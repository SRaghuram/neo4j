/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.LeaderListener;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Set;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Makes the Raft aware of changes to the core topology and vice versa
 */
public class RaftCoreTopologyConnector extends LifecycleAdapter implements CoreTopologyService.Listener, LeaderListener
{
    private final CoreTopologyService coreTopologyService;
    private final RaftMachine raftMachine;
    private final NamedDatabaseId namedDatabaseId;

    public RaftCoreTopologyConnector( CoreTopologyService coreTopologyService, RaftMachine raftMachine, NamedDatabaseId namedDatabaseId )
    {
        this.coreTopologyService = coreTopologyService;
        this.raftMachine = raftMachine;
        this.namedDatabaseId = namedDatabaseId;
    }

    @Override
    public void start()
    {
        coreTopologyService.addLocalCoreTopologyListener( this );
        raftMachine.registerListener( this );
    }

    @Override
    public void stop()
    {
        raftMachine.unregisterListener( this );
        coreTopologyService.removeLocalCoreTopologyListener( this );
    }

    @Override
    public synchronized void onCoreTopologyChange( Set<RaftMemberId> memberIds )
    {
        raftMachine.setTargetMembershipSet( memberIds );
    }

    @Override
    public void onLeaderSwitch( LeaderInfo leaderInfo )
    {
        coreTopologyService.setLeader( leaderInfo, namedDatabaseId );
    }

    @Override
    public void onLeaderStepDown( long stepDownTerm )
    {
        coreTopologyService.handleStepDown( stepDownTerm, namedDatabaseId );
    }

    @Override
    public NamedDatabaseId namedDatabaseId()
    {
        return this.namedDatabaseId;
    }
}
