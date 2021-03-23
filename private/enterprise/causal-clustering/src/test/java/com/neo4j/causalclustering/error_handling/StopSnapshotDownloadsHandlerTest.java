/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import com.neo4j.causalclustering.core.state.snapshot.CoreDownloaderService;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.logging.NullLog;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class StopSnapshotDownloadsHandlerTest
{
    @Test
    void shouldStopCoreDownloaderServiceOnPanic() throws Exception
    {
        // given
        var coreDownloadService = mock( CoreDownloaderService.class );
        var handler = new StopSnapshotDownloadsHandler( coreDownloadService, NullLog.getInstance() );

        // when
        var fooId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        handler.onPanic( new DatabasePanicEvent( fooId, DatabasePanicReason.TEST, new RuntimeException() ) );

        // then
        verify( coreDownloadService ).stop();
    }
}
