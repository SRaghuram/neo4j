/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling.v2.decoding;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import com.neo4j.causalclustering.identity.ClusterId;
import org.junit.Test;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class ClusterIdAwareMessageComposerTest
{
    @Test
    public void shouldThrowExceptionOnConflictingMessageHeaders()
    {
        try
        {
            RaftMessageComposer raftMessageComposer = new RaftMessageComposer( Clock.systemUTC() );

            raftMessageComposer.decode( null, messageCreator( ( a, b ) -> Optional.empty() ), null );
            raftMessageComposer.decode( null, messageCreator( ( a, b ) -> Optional.empty() ), null );
        }
        catch ( IllegalStateException e )
        {
            assertThat( e.getMessage(), containsString( "Pipeline already contains message header waiting to build." ) );
            return;
        }
        fail();
    }

    @Test
    public void shouldThrowExceptionIfNotAllResourcesAreUsed()
    {
        try
        {
            RaftMessageComposer raftMessageComposer = new RaftMessageComposer( Clock.systemUTC() );
            ReplicatedTransaction replicatedTransaction = ReplicatedTransaction.from( new byte[0], DEFAULT_DATABASE_NAME );
            raftMessageComposer.decode( null, replicatedTransaction, null );
            List<Object> out = new ArrayList<>();
            raftMessageComposer.decode( null, messageCreator( ( a, b ) -> Optional.of( dummyRequest() ) ), out );
        }
        catch ( IllegalStateException e )
        {
            assertThat( e.getMessage(),
                    containsString( "was composed without using all resources in the pipeline. Pipeline still contains Replicated contents" ) );
            return;
        }
        fail();
    }

    @Test
    public void shouldThrowExceptionIfUnrecognizedObjectIsFound()
    {
        try
        {
            RaftMessageComposer raftMessageComposer = new RaftMessageComposer( Clock.systemUTC() );

            raftMessageComposer.decode( null, "a string", null );
        }
        catch ( IllegalStateException e )
        {
            assertThat( e.getMessage(), equalTo( "Unexpected object in the pipeline: a string" ) );
            return;
        }
        fail();
    }

    private RaftMessages.PruneRequest dummyRequest()
    {
        return new RaftMessages.PruneRequest( 1 );
    }

    private RaftMessageDecoder.ClusterIdAwareMessageComposer messageCreator( RaftMessageDecoder.LazyComposer composer )
    {
        return new RaftMessageDecoder.ClusterIdAwareMessageComposer( composer, new ClusterId( UUID.randomUUID() ) );
    }
}
