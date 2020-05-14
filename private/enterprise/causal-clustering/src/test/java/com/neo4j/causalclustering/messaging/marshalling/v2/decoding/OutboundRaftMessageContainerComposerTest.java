/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling.v2.decoding;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import com.neo4j.causalclustering.identity.RaftIdFactory;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OutboundRaftMessageContainerComposerTest
{
    @Test
    void shouldThrowExceptionOnConflictingMessageHeaders()
    {
        var raftMessageComposer = new RaftMessageComposer( Clock.systemUTC() );

        raftMessageComposer.decode( null, messageCreator( ( a, b ) -> Optional.empty() ), null );
        var exception =
                assertThrows( IllegalStateException.class, () -> raftMessageComposer.decode( null, messageCreator( ( a, b ) -> Optional.empty() ), null ) );
        assertThat( exception.getMessage(), containsString( "Pipeline already contains message header waiting to build." ) );
    }

    @Test
    void shouldThrowExceptionIfNotAllResourcesAreUsed()
    {
        var raftMessageComposer = new RaftMessageComposer( Clock.systemUTC() );
        var replicatedTransaction =
                ReplicatedTransaction.from( new byte[0], new TestDatabaseIdRepository().defaultDatabase().databaseId() );
        raftMessageComposer.decode( null, replicatedTransaction, null );
        var out = new ArrayList<>();
        var exception = assertThrows( IllegalStateException.class,
                () -> raftMessageComposer.decode( null, messageCreator( ( a, b ) -> Optional.of( dummyRequest() ) ), out ) );
        assertThat( exception.getMessage(),
                containsString( "was composed without using all resources in the pipeline. Pipeline still contains Replicated contents" ) );
    }

    @Test
    void shouldThrowExceptionIfUnrecognizedObjectIsFound()
    {
        var raftMessageComposer = new RaftMessageComposer( Clock.systemUTC() );

        var exception = assertThrows( IllegalStateException.class, () -> raftMessageComposer.decode( null, "a string", null ) );
        assertThat( exception.getMessage(), equalTo( "Unexpected object in the pipeline: a string" ) );
    }

    private RaftMessages.PruneRequest dummyRequest()
    {
        return new RaftMessages.PruneRequest( 1 );
    }

    private RaftMessageDecoder.InboundRaftMessageContainerComposer messageCreator( RaftMessageDecoder.LazyComposer composer )
    {
        return new RaftMessageDecoder.InboundRaftMessageContainerComposer( composer, RaftIdFactory.random() );
    }
}
