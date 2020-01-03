/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import com.neo4j.causalclustering.core.state.machines.token.StorageCommandMarshal;
import com.neo4j.causalclustering.core.state.machines.token.TokenType;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionFactory;
import com.neo4j.causalclustering.core.state.machines.tx.TransactionRepresentationReplicatedTransaction;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.EndOfStreamException;
import com.neo4j.causalclustering.messaging.NetworkReadableChannel;
import com.neo4j.causalclustering.messaging.NetworkWritableChannel;
import com.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import com.neo4j.causalclustering.messaging.marshalling.CoreReplicatedContentMarshalV2;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

import org.neo4j.internal.recordstorage.Command;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.storageengine.api.StorageCommand;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;

class CoreReplicatedContentMarshalV2Test
{
    private final ChannelMarshal<ReplicatedContent> marshal = new CoreReplicatedContentMarshalV2();
    private static final NamedDatabaseId DATABASE_ID = randomNamedDatabaseId();

    @Test
    void shouldMarshalTransactionReference() throws Exception
    {
        ByteBuf buffer = Unpooled.buffer();
        PhysicalTransactionRepresentation representation =
                new PhysicalTransactionRepresentation( Collections.emptyList() );
        representation.setHeader( new byte[]{0}, 1, 1, 1, 1 );

        TransactionRepresentationReplicatedTransaction replicatedTx = ReplicatedTransaction.from( representation, DATABASE_ID );

        assertMarshalingEquality( buffer, replicatedTx );
    }

    @Test
    void shouldMarshalTransactionReferenceWithMissingHeader() throws Exception
    {
        ByteBuf buffer = Unpooled.buffer();
        PhysicalTransactionRepresentation representation =
                new PhysicalTransactionRepresentation( Collections.emptyList() );

        TransactionRepresentationReplicatedTransaction replicatedTx = ReplicatedTransaction.from( representation, DATABASE_ID );

        assertMarshalingEquality( buffer, replicatedTx );
    }

    @Test
    void shouldMarshalMemberSet() throws Exception
    {
        ByteBuf buffer = Unpooled.buffer();
        ReplicatedContent message = new MemberIdSet( asSet(
                new MemberId( UUID.randomUUID() ),
                new MemberId( UUID.randomUUID() )
        ) );

        assertMarshalingEquality( buffer, message );
    }

    @Test
    void shouldMarshalTokenRequest() throws Exception
    {
        ByteBuf buffer = Unpooled.buffer();

        ArrayList<StorageCommand> commands = new ArrayList<>();
        LabelTokenRecord before = new LabelTokenRecord( 0 );
        LabelTokenRecord after = new LabelTokenRecord( 0 );
        after.setInUse( true );
        after.setCreated();
        after.setNameId( 3232 );
        commands.add( new Command.LabelTokenCommand( before, after ) );
        ReplicatedContent message = new ReplicatedTokenRequest( randomNamedDatabaseId().databaseId(),
                TokenType.LABEL, "theLabel", StorageCommandMarshal.commandsToBytes( commands ) );
        assertMarshalingEquality( buffer, message );
    }

    private void assertMarshalingEquality( ByteBuf buffer, ReplicatedContent replicatedTx ) throws IOException, EndOfStreamException
    {
        marshal.marshal( replicatedTx, new NetworkWritableChannel( buffer ) );

        assertThat( marshal.unmarshal( new NetworkReadableChannel( buffer ) ), equalTo( replicatedTx ) );
    }

    private void assertMarshalingEquality( ByteBuf buffer, TransactionRepresentationReplicatedTransaction replicatedTx )
            throws IOException, EndOfStreamException
    {
        marshal.marshal( replicatedTx, new NetworkWritableChannel( buffer ) );

        ReplicatedContent unmarshal = marshal.unmarshal( new NetworkReadableChannel( buffer ) );

        TransactionRepresentation tx = replicatedTx.tx();
        byte[] extraHeader = tx.additionalHeader();
        if ( extraHeader == null )
        {
            // hackishly set additional header to empty array...
            ((PhysicalTransactionRepresentation) tx)
                    .setHeader( new byte[0], tx.getTimeStarted(), tx.getLatestCommittedTxWhenStarted(), tx.getTimeCommitted(), tx.getLeaseId() );
            extraHeader = tx.additionalHeader();
        }
        TransactionRepresentation representation =
                ReplicatedTransactionFactory.extractTransactionRepresentation( (ReplicatedTransaction) unmarshal, extraHeader,
                        new VersionAwareLogEntryReader() );
        assertThat( representation, equalTo( tx ) );
    }
}
