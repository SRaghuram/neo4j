/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.junit.jupiter.api.Test;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.neo4j.backup.impl.BackupProtocolServiceFactory.backupProtocolService;
import static org.neo4j.io.NullOutputStream.NULL_OUTPUT_STREAM;

class BackupProtocolServiceFactoryTest
{

    @Test
    void createDefaultBackupProtocolServiceCreation() throws Exception
    {
        BackupProtocolService protocolService = backupProtocolService();
        assertNotNull( protocolService );

        protocolService.close();
    }

    @Test
    void createBackupProtocolServiceWithOutputStream() throws Exception
    {
        BackupProtocolService protocolService = backupProtocolService( NULL_OUTPUT_STREAM );
        assertNotNull( protocolService );

        protocolService.close();
    }

    @Test
    void createBackupProtocolServiceWithAllPossibleParameters() throws Exception
    {
        PageCache pageCache = mock( PageCache.class );
        BackupProtocolService protocolService =
                backupProtocolService( EphemeralFileSystemRule::new, NullLogProvider.getInstance(), NULL_OUTPUT_STREAM, new Monitors(), pageCache );

        assertNotNull( protocolService );
        protocolService.close();
    }
}
