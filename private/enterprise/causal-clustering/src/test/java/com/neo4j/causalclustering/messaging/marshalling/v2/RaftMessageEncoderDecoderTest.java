/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling.v2;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.RaftMessages.RaftMessage;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolClientInstallerV2;
import com.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolServerInstallerV2;
import com.neo4j.causalclustering.core.replication.DistributedOperation;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.core.replication.session.GlobalSession;
import com.neo4j.causalclustering.core.replication.session.LocalOperationId;
import com.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseRequest;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import com.neo4j.causalclustering.core.state.machines.token.TokenType;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.identity.RaftIdFactory;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.Protocol;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocolVersion;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.stream.ChunkedInput;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.logging.FormattedLogProvider;

import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory.RAFT;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Warning! This test ensures that all raft protocol work as expected in their current implementation. However, it does not know about changes to the
 * protocols that break backward compatibility.
 */
class RaftMessageEncoderDecoderTest
{
    private static final MemberId MEMBER_ID = new MemberId( UUID.randomUUID() );

    private static final List<ApplicationProtocolVersion> PROTOCOLS = ApplicationProtocols.withCategory( RAFT )
            .stream()
            .map( Protocol::implementation )
            .collect( toList() );

    private final EmbeddedChannel outbound = new EmbeddedChannel();
    private final EmbeddedChannel inbound = new EmbeddedChannel();
    private final RaftMessageHandler handler = new RaftMessageHandler();

    private static Stream<Arguments> data()
    {
        var namedDatabaseId = new TestDatabaseIdRepository().defaultDatabase();
        var databaseId = namedDatabaseId.databaseId();
        return setUpParams( new RaftMessage[]{new RaftMessages.Heartbeat( MEMBER_ID, 1, 2, 3 ),
                new RaftMessages.HeartbeatResponse( MEMBER_ID ),
                new RaftMessages.NewEntry.Request( MEMBER_ID, new DummyRequest( new byte[]{1, 2, 3, 4, 5, 6, 7, 8} ) ),
                new RaftMessages.NewEntry.Request( MEMBER_ID, ReplicatedTransaction.from( new byte[]{1, 2, 3, 4, 5, 6, 7, 8}, databaseId ) ),
                new RaftMessages.NewEntry.Request( MEMBER_ID,
                        ReplicatedTransaction.from( new PhysicalTransactionRepresentation( Collections.emptyList() ), namedDatabaseId ) ),
                new RaftMessages.NewEntry.Request( MEMBER_ID,
                        new DistributedOperation(
                                new DistributedOperation(
                                        ReplicatedTransaction.from( new byte[]{1, 2, 3, 4, 5}, databaseId ),
                                        new GlobalSession( UUID.randomUUID(), MEMBER_ID ),
                                        new LocalOperationId( 1, 2 ) ),
                                new GlobalSession( UUID.randomUUID(), MEMBER_ID ), new LocalOperationId( 3, 4 ) ) ),
                new RaftMessages.AppendEntries.Request( MEMBER_ID, 1, 2, 3,
                        new RaftLogEntry[]{
                                new RaftLogEntry( 0, new ReplicatedTokenRequest( databaseId, TokenType.LABEL, "name", new byte[]{2, 3, 4} ) ),
                                new RaftLogEntry( 1, new ReplicatedLeaseRequest( MEMBER_ID, 2, databaseId ) )
                        }, 5 ),
                new RaftMessages.AppendEntries.Response( MEMBER_ID, 1, true, 2, 3 ),
                new RaftMessages.Vote.Request( MEMBER_ID, Long.MAX_VALUE, MEMBER_ID, Long.MIN_VALUE, 1 ),
                new RaftMessages.Vote.Response( MEMBER_ID, 1, true ),
                new RaftMessages.PreVote.Request( MEMBER_ID, Long.MAX_VALUE, MEMBER_ID, Long.MIN_VALUE, 1 ),
                new RaftMessages.PreVote.Response( MEMBER_ID, 1, true ),
                new RaftMessages.LogCompactionInfo( MEMBER_ID, Long.MAX_VALUE, Long.MIN_VALUE )} );
    }

    private static Stream<Arguments> setUpParams( RaftMessage[] messages )
    {
        return Arrays.stream( messages ).flatMap( RaftMessageEncoderDecoderTest::params );
    }

    private static Stream<Arguments> params( RaftMessage raftMessage )
    {
        return PROTOCOLS.stream().map( p -> Arguments.of( raftMessage, p ) );
    }

    @AfterEach
    void cleanUp()
    {
        outbound.finishAndReleaseAll();
        inbound.finishAndReleaseAll();
    }

    @ParameterizedTest( name = "Raft v{1} with message {0}" )
    @MethodSource( "data" )
    void shouldEncodeDecodeRaftMessage( RaftMessage raftMessage, ApplicationProtocolVersion raftProtocol ) throws Exception
    {
        setupChannels( raftProtocol );

        RaftId raftId = RaftIdFactory.random();
        RaftMessages.ReceivedDistributedRaftMessage<RaftMessage> idAwareMessage =
                RaftMessages.ReceivedDistributedRaftMessage.of( Instant.now(), raftId, raftMessage );

        outbound.writeOutbound( idAwareMessage );

        Object o;
        while ( (o = outbound.readOutbound()) != null )
        {
            inbound.writeInbound( o );
        }
        RaftMessages.ReceivedDistributedRaftMessage<RaftMessage> message = handler.getRaftMessage();
        assertEquals( raftId, message.raftId() );
        raftMessageEquals( raftMessage, message.message() );
        assertNull( inbound.readInbound() );
        ReferenceCountUtil.release( handler.msg );
    }

    private void setupChannels( ApplicationProtocolVersion raftProtocol ) throws Exception
    {
        if ( ApplicationProtocols.RAFT_2_0.implementation().equals( raftProtocol ) )
        {
            new RaftProtocolClientInstallerV2( NettyPipelineBuilderFactory.insecure(), Collections.emptyList(),
                    FormattedLogProvider.toOutputStream( System.out ) ).install( outbound );
            new RaftProtocolServerInstallerV2( handler, NettyPipelineBuilderFactory.insecure(), Collections.emptyList(),
                    FormattedLogProvider.toOutputStream( System.out ) ).install( inbound );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown raft protocol " + raftProtocol );
        }
    }

    private static void raftMessageEquals( RaftMessage raftMessage, RaftMessage message ) throws Exception
    {
        if ( raftMessage instanceof RaftMessages.NewEntry.Request )
        {
            assertEquals( message.from(), raftMessage.from() );
            assertEquals( message.type(), raftMessage.type() );
            contentEquals( ((RaftMessages.NewEntry.Request) raftMessage).content(), ((RaftMessages.NewEntry.Request) raftMessage).content() );
        }
        else if ( raftMessage instanceof RaftMessages.AppendEntries.Request )
        {
            assertEquals( message.from(), raftMessage.from() );
            assertEquals( message.type(), raftMessage.type() );
            RaftLogEntry[] entries1 = ((RaftMessages.AppendEntries.Request) raftMessage).entries();
            RaftLogEntry[] entries2 = ((RaftMessages.AppendEntries.Request) message).entries();
            for ( int i = 0; i < entries1.length; i++ )
            {
                RaftLogEntry raftLogEntry1 = entries1[i];
                RaftLogEntry raftLogEntry2 = entries2[i];
                assertEquals( raftLogEntry1.term(), raftLogEntry2.term() );
                contentEquals( raftLogEntry1.content(), raftLogEntry2.content() );
            }
        }
    }

    private static void contentEquals( ReplicatedContent one, ReplicatedContent two ) throws Exception
    {
        if ( one instanceof ReplicatedTransaction )
        {
            ByteBuf buffer1 = Unpooled.buffer();
            ByteBuf buffer2 = Unpooled.buffer();
            encode( buffer1, ((ReplicatedTransaction) one).encode() );
            encode( buffer2, ((ReplicatedTransaction) two).encode() );
            assertEquals( buffer1, buffer2 );
        }
        else if ( one instanceof DistributedOperation )
        {
            assertEquals( ((DistributedOperation) one).globalSession(), ((DistributedOperation) two).globalSession() );
            assertEquals( ((DistributedOperation) one).operationId(), ((DistributedOperation) two).operationId() );
            contentEquals( ((DistributedOperation) one).content(), ((DistributedOperation) two).content() );
        }
        else
        {
            assertEquals( one, two );
        }
    }

    private static void encode( ByteBuf buffer, ChunkedInput<ByteBuf> marshal ) throws Exception
    {
        while ( !marshal.isEndOfInput() )
        {
            ByteBuf tmp = marshal.readChunk( UnpooledByteBufAllocator.DEFAULT );
            if ( tmp != null )
            {
                buffer.writeBytes( tmp );
                tmp.release();
            }
        }
    }

    class RaftMessageHandler extends SimpleChannelInboundHandler<RaftMessages.ReceivedDistributedRaftMessage<RaftMessage>>
    {

        private RaftMessages.ReceivedDistributedRaftMessage<RaftMessage> msg;

        @Override
        protected void channelRead0( ChannelHandlerContext ctx, RaftMessages.ReceivedDistributedRaftMessage<RaftMessage> msg )
        {
            this.msg = msg;
        }

        RaftMessages.ReceivedDistributedRaftMessage<RaftMessage> getRaftMessage()
        {
            return msg;
        }
    }
}
