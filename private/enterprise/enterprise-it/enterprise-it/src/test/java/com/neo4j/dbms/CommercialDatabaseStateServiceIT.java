/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;

import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;

import static com.neo4j.dbms.OperatorState.STOPPED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@EnterpriseDbmsExtension
class CommercialDatabaseStateServiceIT
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private DatabaseManagementService managementService;

    private final TestDatabaseIdRepository idRepository = new TestDatabaseIdRepository();

    @Test
    void shouldReportErrorStatusOnFailedTransition() throws IOException
    {
        var system = (GraphDatabaseAPI) managementService.database( SYSTEM_DATABASE_NAME );
        var dbStateService = system.getDependencyResolver().resolveDependency( DatabaseStateService.class );

        var testId = idRepository.getRaw( "test" );
        managementService.createDatabase( testId.name() );
        var testDb = (GraphDatabaseAPI) managementService.database( testId.name() );
        DatabaseLayout testDbLayout = testDb.databaseLayout();

        managementService.shutdownDatabase( testId.name() );

        fileSystem.deleteFile( testDbLayout.nodeStore() );
        fileSystem.deleteRecursively( testDbLayout.getTransactionLogsDirectory() );

        // when
        managementService.startDatabase( testId.name() );

        // then
        Optional<Throwable> throwable = dbStateService.causeOfFailure( testId );
        assertTrue( throwable.isPresent(), "The state service should have recorded an error when starting a db without key files" );
        assertThat( throwable.get().getMessage(), containsString( "Unable to start") );
        assertEquals( STOPPED.description(), dbStateService.stateOfDatabase( testId ), "The state service should report the db in its stopped state" );
    }
}
