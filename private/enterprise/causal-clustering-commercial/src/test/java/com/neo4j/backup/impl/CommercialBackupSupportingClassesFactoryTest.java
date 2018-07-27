/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.handlers.SslClientPipelineWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.backup.impl.BackupModule;
import org.neo4j.causalclustering.handlers.PipelineWrapper;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CommercialBackupSupportingClassesFactoryTest
{
    private CommercialBackupSupportingClassesFactory subject;

    private final BackupModule backupModule = mock( BackupModule.class );

    private final OutsideWorld outsideWorld = mock( OutsideWorld.class );

    @BeforeEach
    void setup()
    {
        when( backupModule.getOutsideWorld() ).thenReturn( outsideWorld );
        when( backupModule.getLogProvider() ).thenReturn( NullLogProvider.getInstance() );
        subject = new CommercialBackupSupportingClassesFactory( backupModule );
    }

    @Test
    void commercialFactoryReturnsSslProviders()
    {
        Config config = Config.defaults();
        PipelineWrapper appender = subject.createPipelineWrapper( config );
        assertEquals( SslClientPipelineWrapper.class, appender.getClass() );
    }
}
