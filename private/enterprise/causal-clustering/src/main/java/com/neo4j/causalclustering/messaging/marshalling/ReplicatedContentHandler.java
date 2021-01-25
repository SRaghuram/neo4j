/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import java.io.IOException;

import com.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import com.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import com.neo4j.causalclustering.core.replication.DistributedOperation;
import com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseRequest;
import com.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import com.neo4j.causalclustering.core.state.machines.status.StatusRequest;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import com.neo4j.causalclustering.core.state.machines.tx.ByteArrayReplicatedTransaction;
import com.neo4j.causalclustering.core.state.machines.tx.TransactionRepresentationReplicatedTransaction;

public interface ReplicatedContentHandler
{
    void handle( ByteArrayReplicatedTransaction replicatedTransaction ) throws IOException;

    void handle( TransactionRepresentationReplicatedTransaction replicatedTransaction ) throws IOException;

    void handle( MemberIdSet memberIdSet ) throws IOException;

    void handle( ReplicatedTokenRequest replicatedTokenRequest ) throws IOException;

    void handle( NewLeaderBarrier newLeaderBarrier ) throws IOException;

    void handle( ReplicatedLeaseRequest replicatedLeaseRequest ) throws IOException;

    void handle( DistributedOperation distributedOperation ) throws IOException;

    void handle( DummyRequest dummyRequest ) throws IOException;

    void handle( StatusRequest statusRequest ) throws IOException;
}
