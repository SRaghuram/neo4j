/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.storeupgrade;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

import org.neo4j.cli.AdminTool;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.store.format.aligned.PageAligned;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
import static org.neo4j.consistency.checking.full.ConsistencyFlags.DEFAULT;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.helpers.progress.ProgressMonitorFactory.NONE;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;

@TestDirectoryExtension
class StandardToAlignedUpgradeIT
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory directory;

    /**
     * This scenario is used by Aura, where migration starts with a dump from e.g. 3.5 and gets loaded into 4.x and started where
     * migration to aligned format happens automatically on startup. The loaded dump has all possible types of data, indexes and constraints
     * There was an issue where the migration would decide against migrating the properties since it thought that its format hadn't changed
     * and therefore copied those stores instead of migrating them. This would be noticed when later reading from properties where
     * the record format used to read would be "aligned", but the actual property store would be of the "standard" format.
     *
     * The db was originally created using a class in an earlier version called 'CreateDbWithAllPossibleThingsInIt'.
     */
    @Test
    void upgradeStandardToAligned() throws Exception
    {
        // given a clean database home with the loaded dump
        Path homePath = directory.homePath();
        Path confPath = directory.directory( "conf" ); // not needed really, but there for completeness
        Path dumpPath = Paths.get( getClass().getResource( "3.5-standard-db-all.dump" ).toURI() );
        Neo4jLayout neo4jLayout = Neo4jLayout.of( homePath );
        fs.mkdirs( neo4jLayout.databasesDirectory() );
        fs.mkdirs( neo4jLayout.transactionLogsRootDirectory() );
        // neo4j-admin load splits the contents of that old 3.5 db into here ('neo4j' being the default db name):
        // <home>
        //  └─ data
        //      ├─ databases
        //      │   └─ neo4j     <---
        //      └─ transactions
        //          └─ neo4j     <---
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( outBuffer );
        ExecutionContext ctx = new ExecutionContext( homePath, confPath, out, out, fs );
        assertThat( AdminTool.execute( ctx, "load", "--from", dumpPath.toString() ) ).isEqualTo( 0 );

        // when starting the dbms such that migration happens
        DatabaseManagementService dbms = new TestEnterpriseDatabaseManagementServiceBuilder( homePath )
                .setConfig( allow_upgrade, true )
                .setConfig( record_format, PageAligned.LATEST_RECORD_FORMATS.name() )
                .build();

        // then migration should indeed have happened and the data should be readable
        try
        {
            GraphDatabaseAPI db = (GraphDatabaseAPI) dbms.database( DEFAULT_DATABASE_NAME );
            try ( InternalTransaction tx = db.beginTransaction( EXPLICIT, AUTH_DISABLED ) )
            {
                // verify some data
                try ( ResourceIterator<Node> nodes = tx.getAllNodes().iterator() )
                {
                    int numNodes = 0;
                    while ( nodes.hasNext() )
                    {
                        Node node = nodes.next();
                        assertThat( node.hasProperty( "key0" ) ); // they all have this property key
                        numNodes++;
                    }
                    assertThat( numNodes ).isEqualTo( 2494 );
                }
                assertThat( count( tx.getAllRelationships() ) ).isEqualTo( 3659 );
                assertThat( count( tx.schema().getIndexes() ) ).isEqualTo( 11 );
                assertThat( count( tx.schema().getConstraints() ) ).isEqualTo( 3 );
                // simply verify that the indexes have got at least some entries in them and the consistency checker below will
                // verify that the index entries matches the data
                Iterator<IndexDescriptor> indexes = tx.kernelTransaction().schemaRead().indexesGetAll();
                while ( indexes.hasNext() )
                {
                    IndexDescriptor index = indexes.next();
                    assertThat( tx.kernelTransaction().schemaRead().indexSize( index ) ).isGreaterThan( 0 );
                }
            }
        }
        finally
        {
            dbms.shutdown();
        }

        // and here do the check consistency after the db has been closed
        assertThat( new ConsistencyCheckService().runFullConsistencyCheck( neo4jLayout.databaseLayout( DEFAULT_DATABASE_NAME ), Config.defaults(), NONE,
                nullLogProvider(), false, DEFAULT ).isSuccessful() ).isTrue();
    }
}
