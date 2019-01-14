/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.tx;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.replication.Replicator;
import org.neo4j.causalclustering.core.state.Result;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class ReplicatedTransactionCommitProcessTest
{
    private Replicator replicator = mock( Replicator.class );
    private TransactionRepresentation tx = mock( TransactionRepresentation.class );

    @Before
    public void tx()
    {
        when( tx.additionalHeader() ).thenReturn( new byte[]{} );
    }

    @Test
    public void shouldReplicateTransaction() throws Exception
    {
        // given
        long futureTxId = 5L;

        when( replicator.replicate( any( ReplicatedContent.class ) ) ).thenReturn( Result.of( futureTxId ) );
        ReplicatedTransactionCommitProcess commitProcess = new ReplicatedTransactionCommitProcess( replicator, DEFAULT_DATABASE_NAME );

        // when
        long txId = commitProcess.commit( new TransactionToApply( tx ), CommitEvent.NULL, TransactionApplicationMode.EXTERNAL );

        // then
        assertEquals( 5, txId );
    }
}
