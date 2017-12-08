/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.causalclustering.handlers.PipelineHandlerAppender;
import org.neo4j.causalclustering.handlers.SslPipelineHandlerAppender;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.kernel.configuration.Config;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CommercialBackupSupportingClassesFactoryTest
{
    CommercialBackupSupportingClassesFactory subject;

    BackupModule backupModule = mock( BackupModule.class );

    OutsideWorld outsideWorld = mock( OutsideWorld.class );

    @Before
    public void setup()
    {
        when( backupModule.getOutsideWorld() ).thenReturn( outsideWorld );
        subject = new CommercialBackupSupportingClassesFactory( backupModule );
    }

    @Test
    public void commercialFactoryReturnsSslProviders()
    {
        Config config = Config.defaults();
        PipelineHandlerAppender appender = subject.createPipelineHandlerAppender( config );
        assertEquals( SslPipelineHandlerAppender.class, appender.getClass() );
    }
}
