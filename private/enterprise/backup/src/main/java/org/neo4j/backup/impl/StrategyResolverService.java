/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class StrategyResolverService
{
    private final BackupStrategyWrapper haBackupStrategy;
    private final BackupStrategyWrapper ccBackupStrategy;

    StrategyResolverService( BackupStrategyWrapper haBackupStrategy, BackupStrategyWrapper ccBackupStrategy )
    {
        this.haBackupStrategy = haBackupStrategy;
        this.ccBackupStrategy = ccBackupStrategy;
    }

    List<BackupStrategyWrapper> getStrategies( SelectedBackupProtocol selectedBackupProtocol )
    {
        switch ( selectedBackupProtocol )
        {
        case ANY:
            return Arrays.asList( ccBackupStrategy, haBackupStrategy );
        case COMMON:
            return Collections.singletonList( haBackupStrategy );
        case CATCHUP:
            return Collections.singletonList( ccBackupStrategy );
        default:
            throw new IllegalArgumentException( "Unhandled protocol choice: " + selectedBackupProtocol );
        }
    }
}
