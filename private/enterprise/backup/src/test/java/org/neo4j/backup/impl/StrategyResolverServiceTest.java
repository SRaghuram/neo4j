/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class StrategyResolverServiceTest
{

    StrategyResolverService subject;
    BackupStrategyWrapper haBackupStrategy;
    BackupStrategyWrapper ccBackupStrategy;

    @Before
    public void setup()
    {
        haBackupStrategy = mock( BackupStrategyWrapper.class );
        ccBackupStrategy = mock( BackupStrategyWrapper.class );
        subject = new StrategyResolverService( haBackupStrategy, ccBackupStrategy );
    }

    @Test
    public void anyProvidesBothStrategiesCorrectOrder()
    {
        List<BackupStrategyWrapper> result = subject.getStrategies( SelectedBackupProtocol.ANY );
        assertEquals( Arrays.asList( ccBackupStrategy, haBackupStrategy ), result );
    }

    @Test
    public void legacyProvidesBackupProtocol()
    {
        List<BackupStrategyWrapper> result = subject.getStrategies( SelectedBackupProtocol.COMMON );
        assertEquals( Collections.singletonList( haBackupStrategy ), result );
    }

    @Test
    public void catchupProvidesTransactionProtocol()
    {
        List<BackupStrategyWrapper> result = subject.getStrategies( SelectedBackupProtocol.CATCHUP );
        assertEquals( Collections.singletonList( ccBackupStrategy ), result );
    }
}
