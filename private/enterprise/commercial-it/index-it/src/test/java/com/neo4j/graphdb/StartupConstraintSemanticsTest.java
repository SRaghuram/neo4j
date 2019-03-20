/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.graphdb;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;

@ExtendWith( TestDirectoryExtension.class )
class StartupConstraintSemanticsTest
{
    @Inject
    private TestDirectory dir;

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
        GraphDatabaseService graphDb = getCommercialDatabase();
        try
        {
            graphDb.execute( constraintCreationQuery );
        }
        finally
        {
            graphDb.shutdown();
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
                graphDb.shutdown();
            }
        }
    }

    private GraphDatabaseService getCommunityDatabase()
    {
        return new TestGraphDatabaseFactory().newEmbeddedDatabase( dir.databaseDir() );
    }

    private void assertThatCommunityCannotStartOnEnterpriseOnlyConstraint( String constraintCreationQuery, String errorMessage )
    {
        // given
        GraphDatabaseService graphDb = getCommercialDatabase();
        try
        {
            graphDb.execute( constraintCreationQuery );
        }
        finally
        {
            graphDb.shutdown();
        }
        graphDb = null;

        // when
        try
        {
            graphDb = getCommunityDatabase();
            fail( "should have failed to start!" );
        }
        // then
        catch ( Exception e )
        {
            Throwable error = Exceptions.rootCause( e );
            assertThat( error, instanceOf( IllegalStateException.class ) );
            assertEquals( errorMessage, error.getMessage() );
        }
        finally
        {
            if ( graphDb != null )
            {
                graphDb.shutdown();
            }
        }
    }

    private GraphDatabaseService getCommercialDatabase()
    {
        return new TestCommercialGraphDatabaseFactory().newEmbeddedDatabaseBuilder( dir.databaseDir() )
                .setConfig( transaction_logs_root_path, dir.storeDir().getAbsolutePath() )
                .newGraphDatabase();
    }
}
