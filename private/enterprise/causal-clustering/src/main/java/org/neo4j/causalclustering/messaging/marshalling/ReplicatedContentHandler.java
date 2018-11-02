/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging.marshalling;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import org.neo4j.causalclustering.core.replication.DistributedOperation;
import org.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequest;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;

public interface ReplicatedContentHandler
{
    void handle( ReplicatedTransaction replicatedTransaction ) throws IOException;

    void handle( MemberIdSet memberIdSet ) throws IOException;

    void handle( ReplicatedIdAllocationRequest replicatedIdAllocationRequest ) throws IOException;

    void handle( ReplicatedTokenRequest replicatedTokenRequest ) throws IOException;

    void handle( NewLeaderBarrier newLeaderBarrier ) throws IOException;

    void handle( ReplicatedLockTokenRequest replicatedLockTokenRequest ) throws IOException;

    void handle( DistributedOperation distributedOperation ) throws IOException;

    void handle( DummyRequest dummyRequest ) throws IOException;
}
