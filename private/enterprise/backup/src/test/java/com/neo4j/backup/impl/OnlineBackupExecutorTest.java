/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import org.junit.jupiter.api.Test;

import java.nio.file.Paths;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.text.StringContainsInOrder.stringContainsInOrder;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OnlineBackupExecutorTest
{
    @Test
    void nonExistingReportDirectoryRaisesException()
    {
        OnlineBackupExecutor executor = OnlineBackupExecutor.buildDefault();

        var context = OnlineBackupContext.builder()
                                         .withBackupDirectory( Paths.get( "nonExistingReportDirectory" ) );

        BackupExecutionException error = assertThrows( BackupExecutionException.class, () -> executor.executeBackups( context.build() ) );

        assertThat( error.getMessage(), stringContainsInOrder( asList( "Directory '", "nonExistingReportDirectory' does not exist." ) ) );
    }

    @Test
    void nonExistingBackupDirectoryRaisesException()
    {
        OnlineBackupExecutor executor = OnlineBackupExecutor.buildDefault();

        var context = OnlineBackupContext.builder()
                                         .withBackupDirectory( Paths.get( "nonExistingBackupDirectory" ) );

        BackupExecutionException error = assertThrows( BackupExecutionException.class, () -> executor.executeBackups( context.build() ) );

        assertThat( error.getMessage(), stringContainsInOrder( asList( "Directory '", "nonExistingBackupDirectory' does not exist." ) ) );
    }
}
