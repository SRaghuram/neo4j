/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.graphdb;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestDirectoryExtension
class StartupConstraintSemanticsTest
{
    @Inject
    private TestDirectory dir;
    private DatabaseManagementService managementService;

    @Test
    void shouldNotAllowOpeningADatabaseWithPECInCommunityEdition()
    {
        assertThatCommunityCannotStartOnEnterpriseOnlyConstraint( "CREATE CONSTRAINT ON (n:Draconian) ASSERT exists(n.required)",
                StandardConstraintSemantics.ERROR_MESSAGE_EXISTS );
    }

    @Test
    void shouldNotAllowOpeningADatabaseWithNodeKeyInCommunityEdition()
    {
        assertThatCommunityCannotStartOnEnterpriseOnlyConstraint( "CREATE CONSTRAINT ON (n:Draconian) ASSERT (n.required) IS NODE KEY",
                StandardConstraintSemantics.ERROR_MESSAGE_NODE_KEY );
    }

    @Test
    void shouldAllowOpeningADatabaseWithUniqueConstraintInCommunityEdition()
    {
        assertThatCommunityCanStartOnNormalConstraint( "CREATE CONSTRAINT ON (n:Draconian) ASSERT (n.required) IS UNIQUE" );
    }

    private void assertThatCommunityCanStartOnNormalConstraint( String constraintCreationQuery )
    {
        // given
        GraphDatabaseService graphDb = getEnterpriseDatabase();
        try
        {
            try ( Transaction transaction = graphDb.beginTx() )
            {
                transaction.execute( constraintCreationQuery );
                transaction.commit();
            }
        }
        finally
        {
            managementService.shutdown();
        }
        graphDb = null;

        // when
        try
        {
            graphDb = getCommunityDatabase();
            // Should not get exception
        }
        finally
        {
            if ( graphDb != null )
            {
                managementService.shutdown();
            }
        }
    }

    private GraphDatabaseService getCommunityDatabase()
    {
        managementService = new TestDatabaseManagementServiceBuilder( dir.storeDir() ).build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private void assertThatCommunityCannotStartOnEnterpriseOnlyConstraint( String constraintCreationQuery, String errorMessage )
    {
        // given
        GraphDatabaseService graphDb = getEnterpriseDatabase();
        try
        {
            try ( Transaction transaction = graphDb.beginTx() )
            {
                transaction.execute( constraintCreationQuery );
                transaction.commit();
            }
        }
        finally
        {
            managementService.shutdown();
        }
        graphDb = null;

        // when
        try
        {
            graphDb = getCommunityDatabase();
            DatabaseManager<?> databaseManager = ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency( DatabaseManager.class );
            DatabaseContext databaseContext = databaseManager.getDatabaseContext( DEFAULT_DATABASE_NAME ).get();
            assertTrue( databaseContext.isFailed() );
            Throwable error = Exceptions.rootCause( databaseContext.failureCause() );
            assertThat( error, instanceOf( IllegalStateException.class ) );
            assertEquals( errorMessage, error.getMessage() );
        }
        finally
        {
            if ( graphDb != null )
            {
                managementService.shutdown();
            }
        }
    }

    private GraphDatabaseService getEnterpriseDatabase()
    {
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder( dir.storeDir() ).build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }
}
