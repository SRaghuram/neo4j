/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import org.junit.Test;

import java.io.File;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class CopiedStoreRecoveryTest
{
    @Test
    public void shouldThrowIfAlreadyShutdown()
    {
        // Given
        CopiedStoreRecovery copiedStoreRecovery =
                new CopiedStoreRecovery( Config.defaults(), Iterables.empty(), mock( PageCache.class ) );
        copiedStoreRecovery.shutdown();

        try
        {
            // when
            copiedStoreRecovery.recoverCopiedStore( DatabaseLayout.of( new File( "nowhere" ) ) );
            fail( "should have thrown" );
        }
        catch ( DatabaseShutdownException ex )
        {
            // then
            assertEquals( "Abort store-copied store recovery due to database shutdown", ex.getMessage() );
        }
    }
}
