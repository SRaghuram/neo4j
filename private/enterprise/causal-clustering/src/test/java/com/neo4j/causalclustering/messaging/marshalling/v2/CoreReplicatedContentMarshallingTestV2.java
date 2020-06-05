/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling.v2;

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
import com.neo4j.causalclustering.helpers.Buffers;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.BoundedNetworkWritableChannel;
import com.neo4j.causalclustering.messaging.NetworkReadableChannel;
import com.neo4j.causalclustering.messaging.marshalling.CoreReplicatedContentMarshalV2;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Set;
import java.util.UUID;

import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Buffers.Extension
class CoreReplicatedContentMarshallingTestV2
{
    @Inject
    private Buffers buffers;

    static ReplicatedContent[] data()
    {
        var databaseId = new TestDatabaseIdRepository().defaultDatabase().databaseId();
        return new ReplicatedContent[]{new DummyRequest( new byte[]{1, 2, 3} ), ReplicatedTransaction.from( new byte[16 * 1024], databaseId ),
                new MemberIdSet( Set.of( new MemberId( UUID.randomUUID() ) ) ),
                new ReplicatedTokenRequest( databaseId, TokenType.LABEL, "token", new byte[]{'c', 'o', 5} ), new NewLeaderBarrier(),
                new ReplicatedLeaseRequest( new MemberId( UUID.randomUUID() ), 2, databaseId ), new DistributedOperation(
                new DistributedOperation( ReplicatedTransaction.from( new byte[]{1, 2, 3, 4, 5, 6}, databaseId ),
                        new GlobalSession( UUID.randomUUID(), new MemberId( UUID.randomUUID() ) ), new LocalOperationId( 1, 2 ) ),
                new GlobalSession( UUID.randomUUID(), new MemberId( UUID.randomUUID() ) ), new LocalOperationId( 4, 5 ) )};
    }

    @ParameterizedTest
    @MethodSource( "data" )
    public void shouldSerializeAndDeserialize( ReplicatedContent replicatedContent ) throws Exception
    {
        var coreReplicatedContentMarshal = new CoreReplicatedContentMarshalV2();
        var buffer = buffers.buffer();
        var channel = new BoundedNetworkWritableChannel( buffer );
        coreReplicatedContentMarshal.marshal( replicatedContent, channel );

        var readChannel = new NetworkReadableChannel( buffer );
        var result = coreReplicatedContentMarshal.unmarshal( readChannel );

        assertEquals( replicatedContent, result );
    }
}
