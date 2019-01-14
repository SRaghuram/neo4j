/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging.marshalling.v2;

import io.netty.buffer.ByteBuf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashSet;
import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import org.neo4j.causalclustering.core.replication.DistributedOperation;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.replication.session.LocalOperationId;
import org.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import org.neo4j.causalclustering.core.state.machines.token.TokenType;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import org.neo4j.causalclustering.helpers.Buffers;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.BoundedNetworkWritableChannel;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.causalclustering.messaging.marshalling.CoreReplicatedContentMarshalFactory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@RunWith( Parameterized.class )
public class CoreReplicatedContentMarshallingTestV1
{
    @Rule
    public final Buffers buffers = new Buffers();

    @Parameterized.Parameter()
    public ReplicatedContent replicatedContent;

    @Parameterized.Parameters( name = "{0}" )
    public static ReplicatedContent[] data()
    {
        String databaseName = DEFAULT_DATABASE_NAME;
        return new ReplicatedContent[]{new DummyRequest( new byte[]{1, 2, 3} ), ReplicatedTransaction.from( new byte[16 * 1024], databaseName ),
                new MemberIdSet( new HashSet<MemberId>()
                {{
                    add( new MemberId( UUID.randomUUID() ) );
                }} ), new ReplicatedTokenRequest( databaseName, TokenType.LABEL, "token", new byte[]{'c', 'o', 5} ), new NewLeaderBarrier(),
                new ReplicatedLockTokenRequest( new MemberId( UUID.randomUUID() ), 2, databaseName ), new DistributedOperation(
                new DistributedOperation( ReplicatedTransaction.from( new byte[]{1, 2, 3, 4, 5, 6}, databaseName ),
                        new GlobalSession( UUID.randomUUID(), new MemberId( UUID.randomUUID() ) ), new LocalOperationId( 1, 2 ) ),
                new GlobalSession( UUID.randomUUID(), new MemberId( UUID.randomUUID() ) ), new LocalOperationId( 4, 5 ) )};
    }

    @Test
    public void shouldSerializeAndDeserialize() throws Exception
    {
        ChannelMarshal<ReplicatedContent> coreReplicatedContentMarshal = CoreReplicatedContentMarshalFactory.marshalV1( "graph.db" );
        ByteBuf buffer = buffers.buffer();
        BoundedNetworkWritableChannel channel = new BoundedNetworkWritableChannel( buffer );
        coreReplicatedContentMarshal.marshal( replicatedContent, channel );

        NetworkReadableClosableChannelNetty4 readChannel = new NetworkReadableClosableChannelNetty4( buffer );
        ReplicatedContent result = coreReplicatedContentMarshal.unmarshal( readChannel );

        assertEquals( replicatedContent, result );
    }
}
