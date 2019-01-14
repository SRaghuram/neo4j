/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.id;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.transaction.log.InMemoryVersionableReadableClosablePositionAwareChannel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class IdAllocationStateTest
{
    @Test
    public void shouldRoundtripToChannel() throws Exception
    {
        // given
        final IdAllocationState state = new IdAllocationState();

        for ( int i = 1; i <= 3; i++ )
        {
            state.firstUnallocated( IdType.NODE, 1024 * i );
            state.logIndex( i );
        }

        final IdAllocationState.Marshal marshal = new IdAllocationState.Marshal();
        // when
        InMemoryVersionableReadableClosablePositionAwareChannel channel = new
                InMemoryVersionableReadableClosablePositionAwareChannel();
        marshal.marshal( state, channel );
        IdAllocationState unmarshalled = marshal.unmarshal( channel );

        // then
        assertEquals( state, unmarshalled );
    }

    @Test
    public void shouldThrowExceptionForHalfWrittenEntries() throws IOException, EndOfStreamException
    {
        // given
        final IdAllocationState state = new IdAllocationState();

        for ( int i = 1; i <= 3; i++ )
        {
            state.firstUnallocated( IdType.NODE, 1024 * i );
            state.logIndex( i );
        }

        final IdAllocationState.Marshal marshal = new IdAllocationState.Marshal();
        // when
        InMemoryVersionableReadableClosablePositionAwareChannel channel = new
                InMemoryVersionableReadableClosablePositionAwareChannel();
        marshal.marshal( state, channel );
        // append some garbage
        channel.putInt( 1 ).putInt( 2 ).putInt( 3 ).putLong( 4L );
        // read back in the first one
        marshal.unmarshal( channel );

        // the second one will be half read (the ints and longs appended above).
        try
        {
            marshal.unmarshal( channel );
            fail();
        }
        catch ( EndOfStreamException e )
        {
            // expected
        }
    }
}
