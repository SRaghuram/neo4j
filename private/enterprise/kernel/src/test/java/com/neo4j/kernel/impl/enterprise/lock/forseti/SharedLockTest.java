/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.lock.forseti;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class SharedLockTest
{

    @Test
    void shouldUpgradeToUpdateLock()
    {
        // Given
        ForsetiClient clientA = mock( ForsetiClient.class );
        ForsetiClient clientB = mock( ForsetiClient.class );

        SharedLock lock = new SharedLock( clientA );
        lock.acquire( clientB );

        // When
        assertTrue( lock.tryAcquireUpdateLock( clientA ) );

        // Then
        assertThat( lock.numberOfHolders() ).isEqualTo( 2 );
        assertThat( lock.isUpdateLock() ).isEqualTo( true );
    }

    @Test
    void shouldReleaseSharedLock()
    {
        // Given
        ForsetiClient clientA = mock( ForsetiClient.class );
        SharedLock lock = new SharedLock( clientA );

        // When
        assertTrue( lock.release( clientA ) );

        // Then
        assertThat( lock.numberOfHolders() ).isEqualTo( 0 );
        assertThat( lock.isUpdateLock() ).isEqualTo( false );
    }

}
