/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.junit.jupiter.api.Test;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
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
    void createBackupProtocolServiceWithOutputStreamAndconfig() throws Exception
    {
        try ( BackupProtocolService protocolService = backupProtocolService( NULL_OUTPUT_STREAM, Config.defaults() ) )
        {
            assertNotNull( protocolService );
        }
    }

    @Test
    void createBackupProtocolServiceWithAllPossibleParameters() throws Exception
    {
        PageCache pageCache = mock( PageCache.class );
        try ( BackupProtocolService protocolService =
                backupProtocolService( EphemeralFileSystemRule::new, NullLogProvider.getInstance(), NULL_OUTPUT_STREAM, new Monitors(), pageCache ) )
        {
            assertNotNull( protocolService );
        }
    }
}
