/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import org.junit.jupiter.api.Test;

import java.io.File;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class CopiedStoreRecoveryTest
{
    @Test
    void shouldThrowIfAlreadyShutdown()
    {
        CopiedStoreRecovery copiedStoreRecovery = new CopiedStoreRecovery( Config.defaults(), mock( PageCache.class ), new EphemeralFileSystemAbstraction() );
        copiedStoreRecovery.shutdown();

        Exception exception = assertThrows( Exception.class, () -> copiedStoreRecovery.recoverCopiedStore( DatabaseLayout.of( new File( "nowhere" ) ) ) );
        assertEquals( "Abort store-copied store recovery due to database shutdown", exception.getMessage() );
    }
}
