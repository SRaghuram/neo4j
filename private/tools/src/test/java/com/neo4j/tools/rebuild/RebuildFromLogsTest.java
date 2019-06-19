/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.rebuild;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Set;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.checking.InconsistentStoreException;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class, SuppressOutputExtension.class} )
class RebuildFromLogsTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;

    private DatabaseManagementService managementService;

    private static Collection<WorkLog> commands()
    {
        return WorkLog.combinations();
    }

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @ParameterizedTest
    @MethodSource( value = "commands" )
    void shouldRebuildFromLog( WorkLog workLog ) throws Exception, InconsistentStoreException
    {
        // given
        var prototypeLayout = testDirectory.databaseLayout( testDirectory.storeDir( "prototype" ) );
        var rebuildLayout = testDirectory.databaseLayout( testDirectory.storeDir( "rebuild" ) );
        populatePrototype( prototypeLayout, workLog );

        // when
        new RebuildFromLogs( fileSystem ).rebuild( prototypeLayout, rebuildLayout, BASE_TX_ID );

        // then
        assertEquals( DbRepresentation.of( prototypeLayout ), DbRepresentation.of( rebuildLayout ) );
    }

    @ParameterizedTest
    @MethodSource( value = "commands" )
    void failRebuildFromLogIfStoreIsInconsistentAfterRebuild( WorkLog workLog )
    {
        var prototypeLayout = testDirectory.databaseLayout( "prototype" );
        var rebuildLayout = testDirectory.databaseLayout( "rebuild" );
        populatePrototype( prototypeLayout, workLog );

        // when
        assertThrows( InconsistentStoreException.class, () ->
        {
            var rebuildFromLogs = new TestRebuildFromLogs( fileSystem );
            rebuildFromLogs.rebuild( prototypeLayout, rebuildLayout, BASE_TX_ID );
        } );
    }

    @ParameterizedTest
    @MethodSource( value = "commands" )
    void shouldRebuildFromLogUpToATx( WorkLog workLog ) throws Exception, InconsistentStoreException
    {
        // given
        var prototypeLayout = testDirectory.databaseLayout( "prototype" );
        var copyLayout = testDirectory.databaseLayout( "copy" );
        var rebuildLayout = testDirectory.databaseLayout( "rebuild" );
        long txId = populatePrototype( prototypeLayout, workLog );

        FileUtils.copyRecursively( prototypeLayout.databaseDirectory(), copyLayout.databaseDirectory() );
        FileUtils.copyRecursively( prototypeLayout.getTransactionLogsDirectory(), copyLayout.getTransactionLogsDirectory() );
        GraphDatabaseAPI db = db( copyLayout );
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
        finally
        {
            managementService.shutdown();
        }

        // when
        new RebuildFromLogs( fileSystem ).rebuild( copyLayout, rebuildLayout, txId );

        // then
        assertEquals( DbRepresentation.of( prototypeLayout ), DbRepresentation.of( rebuildLayout ) );
    }

    private long populatePrototype( DatabaseLayout databaseLayout, WorkLog workLog )
    {
        GraphDatabaseAPI prototype = db( databaseLayout );
        long txId;
        try
        {
            for ( Transaction transaction : workLog.transactions() )
            {
                transaction.applyTo( prototype );
            }
        }
        finally
        {
            txId = prototype.getDependencyResolver().resolveDependency( MetaDataStore.class ).getLastCommittedTransactionId();
            managementService.shutdown();
        }
        return txId;
    }

    private GraphDatabaseAPI db( DatabaseLayout databaseLayout )
    {
        managementService = new TestDatabaseManagementServiceBuilder( databaseLayout.getStoreLayout().storeDirectory() )
                .setConfig( GraphDatabaseSettings.default_database, databaseLayout.getDatabaseName() )
                .build();
        return (GraphDatabaseAPI) managementService.database( databaseLayout.getDatabaseName() );
    }

    enum Transaction
    {
        CREATE_NODE
                {
                    @Override
                    void applyTx( GraphDatabaseService graphDb )
                    {
                        graphDb.createNode();
                    }
                },
        CREATE_NODE_WITH_PROPERTY
                {
                    @Override
                    void applyTx( GraphDatabaseService graphDb )
                    {
                        graphDb.createNode().setProperty( name(), "value" );
                    }
                },
        SET_PROPERTY( CREATE_NODE )
                {
                    @Override
                    void applyTx( GraphDatabaseService graphDb )
                    {
                        firstNode( graphDb ).setProperty( name(), "value" );
                    }
                },
        CHANGE_PROPERTY( CREATE_NODE_WITH_PROPERTY )
                {
                    @Override
                    void applyTx( GraphDatabaseService graphDb )
                    {
                        ResourceIterable<Node> nodes = graphDb.getAllNodes();
                        try ( ResourceIterator<Node> iterator = nodes.iterator() )
                        {
                            while ( iterator.hasNext() )
                            {
                                Node node = iterator.next();
                                if ( "value".equals( node.getProperty( CREATE_NODE_WITH_PROPERTY.name(), null ) ) )
                                {
                                    node.setProperty( CREATE_NODE_WITH_PROPERTY.name(), "other" );
                                    break;
                                }
                            }
                        }
                    }
                };

        private static Node firstNode( GraphDatabaseService graphDb )
        {
            return Iterables.firstOrNull( graphDb.getAllNodes() );
        }

        private final Transaction[] dependencies;

        Transaction( Transaction... dependencies )
        {
            this.dependencies = dependencies;
        }

        void applyTo( GraphDatabaseService graphDb )
        {
            try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
            {
                applyTx( graphDb );

                tx.success();
            }
        }

        void applyTx( GraphDatabaseService graphDb )
        {
        }
    }

    static class WorkLog
    {
        static final WorkLog BASE = new WorkLog( EnumSet.noneOf( Transaction.class ) );
        final EnumSet<Transaction> transactions;

        WorkLog( EnumSet<Transaction> transactions )
        {
            this.transactions = transactions;
        }

        @Override
        public boolean equals( Object that )
        {
            return this == that ||
                   that instanceof WorkLog &&
                   transactions.equals( ((WorkLog) that).transactions );
        }

        @Override
        public int hashCode()
        {
            return transactions.hashCode();
        }

        @Override
        public String toString()
        {
            return transactions.toString();
        }

        Transaction[] transactions()
        {
            return transactions.toArray( new Transaction[0] );
        }

        static Set<WorkLog> combinations()
        {
            Set<WorkLog> combinations = Collections.newSetFromMap( new LinkedHashMap<>() );
            for ( Transaction transaction : Transaction.values() )
            {
                combinations.add( BASE.extend( transaction ) );
            }
            for ( Transaction transaction : Transaction.values() )
            {
                for ( WorkLog combination : new ArrayList<>( combinations ) )
                {
                    combinations.add( combination.extend( transaction ) );
                }
            }
            return combinations;
        }

        private WorkLog extend( Transaction transaction )
        {
            EnumSet<Transaction> base = EnumSet.copyOf( transactions );
            Collections.addAll( base, transaction.dependencies );
            base.add( transaction );
            return new WorkLog( base );
        }
    }

    private static class TestRebuildFromLogs extends RebuildFromLogs
    {
        TestRebuildFromLogs( FileSystemAbstraction fs )
        {
            super( fs );
        }

        @Override
        void checkConsistency( DatabaseLayout targetLayout, PageCache pageCache ) throws InconsistentStoreException
        {
            throw new InconsistentStoreException( new ConsistencySummaryStatistics() );
        }
    }
}
