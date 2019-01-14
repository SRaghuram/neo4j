/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import com.hazelcast.core.HazelcastInstance;
import org.junit.Test;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RobustHazelcastWrapperTest
{
    @Test
    public void shouldReconnectIfHazelcastConnectionInvalidated() throws Exception
    {
        // given
        HazelcastConnector connector = mock( HazelcastConnector.class );
        HazelcastInstance hzInstance = mock( HazelcastInstance.class );

        when( connector.connectToHazelcast() ).thenReturn( hzInstance );

        RobustHazelcastWrapper hzWrapper = new RobustHazelcastWrapper( connector );

        // when
        hzWrapper.perform( hz ->
        { /* nothing*/ } );

        // then
        verify( connector, times( 1 ) ).connectToHazelcast();

        // then
        try
        {
            hzWrapper.perform( hz ->
            {
                throw new com.hazelcast.core.HazelcastInstanceNotActiveException();
            } );
            fail();
        }
        catch ( HazelcastInstanceNotActiveException e )
        {
            // expected
        }

        // when
        hzWrapper.perform( hz ->
        { /* nothing*/ } );
        hzWrapper.perform( hz ->
        { /* nothing*/ } );

        // then
        verify( connector, times( 2 ) ).connectToHazelcast();
    }
}
