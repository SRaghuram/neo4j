/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j;

import com.neo4j.commercial.edition.factory.CommercialGraphDatabaseFactory;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.ConsistencyCheckTool;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
class CompositeConstraintIT
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void compositeNodeKeyConstraintUpdate() throws Exception
    {
        GraphDatabaseService database = new CommercialGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( testDirectory.storeDir() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .newGraphDatabase();

        Label label = Label.label( "label" );

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( label );
            node.setProperty( "b", (short) 3 );
            node.setProperty( "a", new double[]{0.6, 0.4, 0.2} );
            transaction.success();
        }

        try ( Transaction transaction = database.beginTx() )
        {
            String query = format( "CREATE CONSTRAINT ON (n:%s) ASSERT (n.%s,n.%s) IS NODE KEY", label.name(), "a", "b" );
            database.execute( query );
            transaction.success();
        }

        awaitIndex( database );

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( label );
            node.setProperty( "a", (short) 7 );
            node.setProperty( "b", new double[]{0.7, 0.5, 0.3} );
            transaction.success();
        }
        database.shutdown();

        ConsistencyCheckService.Result consistencyCheckResult = checkDbConsistency( testDirectory.storeDir() );
        assertTrue( consistencyCheckResult.isSuccessful(), "Database is consistent" );
    }

    private static ConsistencyCheckService.Result checkDbConsistency( File databaseDirectory )
            throws ConsistencyCheckTool.ToolFailureException
    {
        return ConsistencyCheckTool.runConsistencyCheckTool( new String[]{databaseDirectory.getAbsolutePath()}, System.out, System.err );
    }

    private static void awaitIndex( GraphDatabaseService database )
    {
        try ( Transaction tx = database.beginTx() )
        {
            database.schema().awaitIndexesOnline( 2, TimeUnit.MINUTES );
            tx.success();
        }
    }
}
