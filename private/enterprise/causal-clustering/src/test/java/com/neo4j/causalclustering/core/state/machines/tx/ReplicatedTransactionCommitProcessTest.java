/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.core.replication.ReplicationFailureException;
import com.neo4j.causalclustering.core.replication.Replicator;
import com.neo4j.causalclustering.core.state.Result;
import com.neo4j.causalclustering.error_handling.Panicker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class ReplicatedTransactionCommitProcessTest
{
    private Replicator replicator = mock( Replicator.class );
    private TransactionRepresentation tx = mock( TransactionRepresentation.class );

    @BeforeEach
    void tx()
    {
        when( tx.additionalHeader() ).thenReturn( new byte[]{} );
    }

    @Test
    void shouldReplicateTransaction() throws Exception
    {
        // given
        long resultTxId = 5L;

        when( replicator.replicate( any( ReplicatedContent.class ) ) ).thenReturn( Result.of( resultTxId ) );
        ReplicatedTransactionCommitProcess commitProcess = new ReplicatedTransactionCommitProcess( replicator, DEFAULT_DATABASE_NAME, mock( Panicker.class ) );

        // when
        long txId = commitProcess.commit( new TransactionToApply( tx ), CommitEvent.NULL, TransactionApplicationMode.EXTERNAL );

        // then
        assertEquals( 5, txId );
    }

    @Test
    void shouldPanicOnUnexpectedException() throws ReplicationFailureException
    {
        // given
        IllegalArgumentException resultError = new IllegalArgumentException( "Result error" );
        Panicker panicker = mock( Panicker.class );

        when( replicator.replicate( any( ReplicatedContent.class ) ) ).thenReturn( Result.of( resultError ) );
        ReplicatedTransactionCommitProcess commitProcess = new ReplicatedTransactionCommitProcess( replicator, DEFAULT_DATABASE_NAME, panicker );

        // when
        RuntimeException commitException = assertThrows( RuntimeException.class,
                () -> commitProcess.commit( new TransactionToApply( tx ), CommitEvent.NULL, TransactionApplicationMode.EXTERNAL ) );

        // then
        assertEquals( resultError, commitException.getCause() );
        verify( panicker ).panic( resultError );
    }
}
