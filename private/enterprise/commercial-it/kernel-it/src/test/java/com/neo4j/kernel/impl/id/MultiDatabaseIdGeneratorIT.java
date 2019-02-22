/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.id;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.store.id.IdController;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

@ExtendWith( TestDirectoryExtension.class )
class MultiDatabaseIdGeneratorIT
{
    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;
    private GraphDatabaseFacade firstDatabase;
    private GraphDatabaseFacade secondDatabase;
    private IdGeneratorFactory firstIdGeneratorFactory;
    private IdGeneratorFactory secondIdGeneratorFactory;

    @BeforeEach
    void setUp()
    {
        database = new TestCommercialGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.databaseDir() );
        DatabaseManager databaseManager = getDatabaseManager();
        firstDatabase = getDefaultDatabase( databaseManager );
        secondDatabase = startSecondDatabase( databaseManager );
        firstIdGeneratorFactory = getIdGeneratorFactory( firstDatabase );
        secondIdGeneratorFactory = getIdGeneratorFactory( secondDatabase );
    }

    @AfterEach
    void tearDown()
    {
        database.shutdown();
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
            firstNodeIdGenerator.freeId( idToReuse );
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

    private static GraphDatabaseFacade startSecondDatabase( DatabaseManager databaseManager )
    {
        return databaseManager.createDatabase( "second.db" ).getDatabaseFacade();
    }

    private static GraphDatabaseFacade getDefaultDatabase( DatabaseManager databaseManager )
    {
        return databaseManager.getDatabaseContext( Config.defaults().get( GraphDatabaseSettings.active_database ) )
                .orElseThrow( () -> new IllegalStateException( "Default database not found." ) )
                .getDatabaseFacade();
    }

    private static IdGeneratorFactory getIdGeneratorFactory( GraphDatabaseFacade database )
    {
        return database.getDependencyResolver().resolveDependency( IdGeneratorFactory.class );
    }

    private DatabaseManager getDatabaseManager()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }
}
