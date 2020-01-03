/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import org.junit.jupiter.api.Test;

import java.io.File;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.storageengine.api.StorageEngineFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class CopiedStoreRecoveryTest
{
    @Test
    void shouldThrowIfAlreadyShutdown()
    {
        CopiedStoreRecovery copiedStoreRecovery = new CopiedStoreRecovery( mock( PageCache.class ), new EphemeralFileSystemAbstraction(),
                StorageEngineFactory.selectStorageEngine() );
        copiedStoreRecovery.shutdown();

        Exception exception = assertThrows( Exception.class,
                () -> copiedStoreRecovery.recoverCopiedStore( Config.defaults(), DatabaseLayout.ofFlat( new File( "nowhere" ) ) ) );
        assertEquals( "Abort store-copied store recovery due to database shutdown", exception.getMessage() );
    }
}
