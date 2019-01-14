/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.io.NullOutputStream.NULL_OUTPUT_STREAM;

class BackupProtocolServiceTest
{

    @Test
    void closePageCacheContainerOnClose() throws Exception
    {
        PageCache pageCache = mock( PageCache.class );
        BackupPageCacheContainer container = BackupPageCacheContainer.of( pageCache );
        BackupProtocolService protocolService =
                new BackupProtocolService( EphemeralFileSystemAbstraction::new, NullLogProvider.getInstance(), NULL_OUTPUT_STREAM, new Monitors(), container );
        protocolService.close();

        verify( pageCache ).close();
    }
}
