/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import com.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import com.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import com.neo4j.causalclustering.core.replication.DistributedOperation;
import com.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import com.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequest;
import com.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import com.neo4j.causalclustering.core.state.machines.tx.ByteArrayReplicatedTransaction;
import com.neo4j.causalclustering.core.state.machines.tx.TransactionRepresentationReplicatedTransaction;

import java.io.IOException;

public interface ReplicatedContentHandler
{
    void handle( ByteArrayReplicatedTransaction replicatedTransaction ) throws IOException;

    void handle( TransactionRepresentationReplicatedTransaction replicatedTransaction ) throws IOException;

    void handle( MemberIdSet memberIdSet ) throws IOException;

    void handle( ReplicatedIdAllocationRequest replicatedIdAllocationRequest ) throws IOException;

    void handle( ReplicatedTokenRequest replicatedTokenRequest ) throws IOException;

    void handle( NewLeaderBarrier newLeaderBarrier ) throws IOException;

    void handle( ReplicatedLockTokenRequest replicatedLockTokenRequest ) throws IOException;

    void handle( DistributedOperation distributedOperation ) throws IOException;

    void handle( DummyRequest dummyRequest ) throws IOException;
}
