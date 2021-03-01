/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import com.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import com.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import com.neo4j.causalclustering.core.replication.DistributedOperation;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.core.replication.session.GlobalSession;
import com.neo4j.causalclustering.core.replication.session.LocalOperationId;
import com.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseRequest;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import com.neo4j.causalclustering.core.state.machines.token.TokenType;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

class CoreReplicatedContentMarshallingV2Test implements BaseMarshalTest<ReplicatedContent>
{

    @Override
    public Collection<ReplicatedContent> originals()
    {
        var databaseId = new TestDatabaseIdRepository().defaultDatabase().databaseId();
        var raftMemberId = IdFactory.randomRaftMemberId();
        var globalSession = new GlobalSession( UUID.randomUUID(), raftMemberId );
        return List.of( new DummyRequest( new byte[]{1, 2, 3} ), ReplicatedTransaction.from( new byte[16 * 1024], databaseId ),
                        new MemberIdSet( Set.of( raftMemberId ) ),
                        new ReplicatedTokenRequest( databaseId, TokenType.LABEL, "token", new byte[]{'c', 'o', 5} ), new NewLeaderBarrier(),
                        new ReplicatedLeaseRequest( raftMemberId, 2, databaseId ),
                        new DistributedOperation(
                                new DistributedOperation(
                                        ReplicatedTransaction.from( new byte[]{1, 2, 3, 4, 5, 6}, databaseId ),
                                        globalSession,
                                        new LocalOperationId( 1, 2 ) ),
                                globalSession,
                                new LocalOperationId( 4, 5 ) ) );
    }

    @Override
    public ChannelMarshal<ReplicatedContent> marshal()
    {
        return new CoreReplicatedContentMarshal( LogEntryWriterFactory.LATEST );
    }
}
