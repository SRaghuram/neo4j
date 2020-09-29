/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import picocli.CommandLine;

import java.io.IOException;

import org.neo4j.commandline.dbms.StoreInfoCommand;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.recovery.RecoveryHelpers;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
public class StoreInfoCommandIT extends AbstractCommandIT
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private DatabaseManagementService managementService;

    @Test
    void storeInfoCorrectlyDetectsTheNeedForRecovery() throws IOException
    {
        // given
        managementService.createDatabase( "foo" );
        var fooDb = (GraphDatabaseAPI) managementService.database( "foo" );

        try ( var tx = fooDb.beginTx() )
        {
            tx.createNode();
            tx.commit();
        }

        managementService.shutdownDatabase( "foo" );
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( fooDb.databaseLayout(), fileSystem );

        var format = RecordFormatSelector.defaultFormat();
        var expected = "Database name:                foo" + System.lineSeparator() +
                       "Database in use:              false" + System.lineSeparator() +
                       "Store format version:         " + format.storeVersion() + System.lineSeparator() +
                       "Store format introduced in:   " + format.introductionVersion() + System.lineSeparator() +
                       "Last committed transaction id:2" + System.lineSeparator() +
                       "Store needs recovery:         true";

        // when
        var command = new StoreInfoCommand( getExtensionContext() );
        CommandLine.populateCommand( command, fooDb.databaseLayout().databaseDirectory().toString() );
        command.execute();

        // then
        assertTrue( out.containsMessage( expected ) );
    }
}
