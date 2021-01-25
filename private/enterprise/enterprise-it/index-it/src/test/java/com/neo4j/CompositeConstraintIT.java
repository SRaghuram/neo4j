/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.SuppressOutputExtension;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.progress.ProgressMonitorFactory.NONE;

@Neo4jLayoutExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class CompositeConstraintIT
{
    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    void compositeNodeKeyConstraintUpdate() throws Exception
    {
        DatabaseManagementService managementService =
                new TestEnterpriseDatabaseManagementServiceBuilder( databaseLayout ).build();
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );

        Label label = Label.label( "label" );

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = transaction.createNode( label );
            node.setProperty( "b", (short) 3 );
            node.setProperty( "a", new double[]{0.6, 0.4, 0.2} );
            transaction.commit();
        }

        try ( Transaction transaction = database.beginTx() )
        {
            String query = format( "CREATE CONSTRAINT ON (n:%s) ASSERT (n.%s,n.%s) IS NODE KEY", label.name(), "a", "b" );
            transaction.execute( query );
            transaction.commit();
        }

        awaitIndex( database );

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = transaction.createNode( label );
            node.setProperty( "a", (short) 7 );
            node.setProperty( "b", new double[]{0.7, 0.5, 0.3} );
            transaction.commit();
        }
        managementService.shutdown();

        ConsistencyCheckService.Result consistencyCheckResult = checkDbConsistency( databaseLayout );
        assertTrue( consistencyCheckResult.isSuccessful(), "Database is consistent" );
    }

    private static ConsistencyCheckService.Result checkDbConsistency( DatabaseLayout databaseLayout )
            throws ConsistencyCheckIncompleteException
    {
        ConsistencyCheckService service = new ConsistencyCheckService();
        return service.runFullConsistencyCheck( databaseLayout, Config.defaults(), NONE, NullLogProvider.getInstance(), false );
    }

    private static void awaitIndex( GraphDatabaseService database )
    {
        try ( Transaction tx = database.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 2, TimeUnit.MINUTES );
            tx.commit();
        }
    }
}
