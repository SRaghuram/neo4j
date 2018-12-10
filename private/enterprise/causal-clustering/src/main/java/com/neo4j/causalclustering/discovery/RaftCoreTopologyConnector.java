/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.LeaderListener;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Set;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Makes the Raft aware of changes to the core topology and vice versa
 */
public class RaftCoreTopologyConnector extends LifecycleAdapter implements CoreTopologyService.Listener, LeaderListener
{
    private final CoreTopologyService coreTopologyService;
    private final RaftMachine raftMachine;
    private final String dbName;

    public RaftCoreTopologyConnector( CoreTopologyService coreTopologyService, RaftMachine raftMachine, String dbName )
    {
        this.coreTopologyService = coreTopologyService;
        this.raftMachine = raftMachine;
        this.dbName = dbName;
    }

    @Override
    public void start()
    {
        coreTopologyService.addLocalCoreTopologyListener( this );
        raftMachine.registerListener( this );
    }

    @Override
    public synchronized void onCoreTopologyChange( CoreTopology coreTopology )
    {
        Set<MemberId> targetMembers = coreTopology.members().keySet();
        raftMachine.setTargetMembershipSet( targetMembers );
    }

    @Override
    public void onLeaderSwitch( LeaderInfo leaderInfo )
    {
        coreTopologyService.setLeader( leaderInfo, dbName );
    }

    @Override
    public void onLeaderStepDown( long stepDownTerm )
    {
        coreTopologyService.handleStepDown( stepDownTerm, dbName );
    }

    @Override
    public String dbName()
    {
        return this.dbName;
    }
}
