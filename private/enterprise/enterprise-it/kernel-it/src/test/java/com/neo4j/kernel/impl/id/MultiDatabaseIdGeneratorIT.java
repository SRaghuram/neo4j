/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.id;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdRange;
import org.neo4j.internal.id.IdType;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestDirectoryExtension
class MultiDatabaseIdGeneratorIT
{
    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseFacade firstDatabase;
    private GraphDatabaseFacade secondDatabase;
    private IdGeneratorFactory firstIdGeneratorFactory;
    private IdGeneratorFactory secondIdGeneratorFactory;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp() throws DatabaseExistsException
    {
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.storeDir() ).build();
        firstDatabase = (GraphDatabaseFacade) managementService.database( DEFAULT_DATABASE_NAME );
        var secondDb = "second";
        managementService.createDatabase( secondDb );
        secondDatabase = (GraphDatabaseFacade) managementService.database( secondDb );
        firstIdGeneratorFactory = getIdGeneratorFactory( firstDatabase );
        secondIdGeneratorFactory = getIdGeneratorFactory( secondDatabase );
    }

    @AfterEach
    void tearDown()
    {
        managementService.shutdown();
    }

    @Test
    void differentDatabaseHaveDifferentIdGeneratorFactories()
    {
        assertNotSame( firstIdGeneratorFactory, secondIdGeneratorFactory );
    }

    @Test
    void differentDatabasesHaveDifferentIdControllers()
    {
        IdController firstController = getDatabaseIdController( firstDatabase );
        IdController secondController = getDatabaseIdController( secondDatabase );
        assertNotSame( firstController, secondController);
    }

    @Test
    void acquireSameIdsInTwoDatabaseSimultaneously()
    {
        IdGenerator firstNodeIdGenerator = firstIdGeneratorFactory.get( IdType.NODE );
        IdGenerator secondNodeIdGenerator = secondIdGeneratorFactory.get( IdType.NODE );

        assertEquals( firstNodeIdGenerator.getHighId(), secondNodeIdGenerator.getHighId() );
        assertEquals( firstNodeIdGenerator.nextId(), secondNodeIdGenerator.nextId() );
    }

    @Test
    void releasingIdsOnOneDatabaseDoesNotInfluenceAnother()
    {
        IdGenerator firstNodeIdGenerator = firstIdGeneratorFactory.get( IdType.NODE );
        IdGenerator secondNodeIdGenerator = secondIdGeneratorFactory.get( IdType.NODE );

        long requestedSize = 100;
        int idsToReuse = 10;

        IdRange batch = firstNodeIdGenerator.nextIdBatch( (int) requestedSize );
        assertThat( firstNodeIdGenerator.getNumberOfIdsInUse(), greaterThanOrEqualTo( requestedSize ) );
        for ( long idToReuse = batch.getRangeStart(); idToReuse < batch.getRangeStart() + idsToReuse; idToReuse++ )
        {
            try ( IdGenerator.ReuseMarker marker = firstNodeIdGenerator.reuseMarker() )
            {
                marker.markFree( idToReuse );
            }
            try ( IdGenerator.CommitMarker marker = firstNodeIdGenerator.commitMarker() )
            {
                marker.markDeleted( idToReuse );
            }
        }

        getDatabaseIdController( firstDatabase ).maintenance();
        getDatabaseIdController( secondDatabase ).maintenance();

        assertEquals( 0, secondNodeIdGenerator.getDefragCount() );
        assertEquals( idsToReuse, firstNodeIdGenerator.getDefragCount() );
    }

    private static IdController getDatabaseIdController( GraphDatabaseFacade firstDatabase )
    {
        return firstDatabase.getDependencyResolver().resolveDependency( IdController.class );
    }

    private static IdGeneratorFactory getIdGeneratorFactory( GraphDatabaseFacade database )
    {
        return database.getDependencyResolver().resolveDependency( IdGeneratorFactory.class );
    }
}
