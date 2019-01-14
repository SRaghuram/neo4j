/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.snapshot;

import org.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import org.neo4j.causalclustering.core.state.machines.id.IdAllocationState;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenState;
import org.neo4j.causalclustering.core.state.storage.StateMarshal;
import org.neo4j.causalclustering.identity.MemberId;

public enum CoreStateType
{
    LOCK_TOKEN( new ReplicatedLockTokenState.Marshal( new MemberId.Marshal() ) ),
    SESSION_TRACKER( new GlobalSessionTrackerState.Marshal( new MemberId.Marshal() ) ),
    ID_ALLOCATION( new IdAllocationState.Marshal() ),
    RAFT_CORE_STATE( new RaftCoreState.Marshal() );

    public final StateMarshal marshal;

    CoreStateType( StateMarshal marshal )
    {
        this.marshal = marshal;
    }
}
