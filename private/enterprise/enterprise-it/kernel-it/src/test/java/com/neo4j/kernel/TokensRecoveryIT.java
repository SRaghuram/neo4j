/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.configuration.OnlineBackupSettings.online_backup_enabled;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterables.count;

@EphemeralTestDirectoryExtension
class TokensRecoveryIT
{
    private static final Label LABEL = Label.label( "Label" );

    @Inject
    private TestDirectory testDirectory;
    @Inject
    private EphemeralFileSystemAbstraction fs;
    private DatabaseManagementService managementService;

    @Test
    void tokenCreateCommandsMustUpdateTokenHoldersDuringRecoveryWithFulltextIndex()
    {
        // The reason is that other components that are participating in recovery, such as the fulltext index provider, and the property-based schema store,
        // will want to read tokens during recovery, and some of those tokens might be coming from the transactions that we are recovering.
        GraphDatabaseService db = startDatabase( fs );

        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CALL db.index.fulltext.createNodeIndex('nodes', ['Label'], ['prop'] )" ).close();
            tx.commit();
        }
        // Crash the database - the store files are all empty and everything will be recovered from the logs.
        EphemeralFileSystemAbstraction crashedFs = fs.snapshot();
        managementService.shutdown();

        // Recover and start the database again.
        db = startDatabase( crashedFs );

        try ( Transaction tx = db.beginTx() )
        {
            // Now we should see our index still being there and healthy.
            assertThat( count( tx.schema().getIndexes() ) ).isEqualTo( 1L );
            assertThat( count( tx.schema().getIndexes( LABEL ) ) ).isEqualTo( 1L );
            tx.commit();
        }
        managementService.shutdown();
    }

    @Test
    void tokenCreateCommandsMustUpdateTokenHoldersDuringRecoveryWithConstraint()
    {
        // The reason is that other components that are participating in recovery, such as the fulltext index provider, and the property-based schema store,
        // will want to read tokens during recovery, and some of those tokens might be coming from the transactions that we are recovering.
        GraphDatabaseService db = startDatabase( fs );

        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().constraintFor( LABEL ).assertPropertyIsUnique( "prop" ).create();
            tx.commit();
        }
        // Crash the database - the store files are all empty and everything will be recovered from the logs.
        EphemeralFileSystemAbstraction crashedFs = fs.snapshot();
        managementService.shutdown();

        // Recover and start the database again.
        db = startDatabase( crashedFs );

        try ( Transaction tx = db.beginTx() )
        {
            // Now we should see our index still being there and healthy.
            assertThat( count( tx.schema().getIndexes() ) ).isEqualTo( 1L );
            assertThat( count( tx.schema().getIndexes( LABEL ) ) ).isEqualTo( 1L );
            tx.commit();
        }
        managementService.shutdown();
    }

    private GraphDatabaseService startDatabase( EphemeralFileSystemAbstraction fs )
    {
        DatabaseManagementServiceBuilder builder = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() ).setFileSystem( fs );
        builder.setConfig( online_backup_enabled, false );
        builder.setConfig( GraphDatabaseSettings.check_point_policy, GraphDatabaseSettings.CheckpointPolicy.PERIODIC );
        managementService = builder.build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }
}
