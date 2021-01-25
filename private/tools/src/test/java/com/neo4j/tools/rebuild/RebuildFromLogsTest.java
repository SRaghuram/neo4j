/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.rebuild;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
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
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.SuppressOutputExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

@Neo4jLayoutExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class RebuildFromLogsTest
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private Neo4jLayout neo4jLayout;

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
        var prototypeLayout = neo4jLayout.databaseLayout( "prototype" );
        var rebuildLayout = neo4jLayout.databaseLayout( "rebuild" );
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
        var prototypeLayout = neo4jLayout.databaseLayout( "prototype" );
        var rebuildLayout = neo4jLayout.databaseLayout( "rebuild" );
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
        var prototypeLayout = neo4jLayout.databaseLayout( "prototype" );
        var copyLayout = neo4jLayout.databaseLayout( "copy" );
        var rebuildLayout = neo4jLayout.databaseLayout( "rebuild" );
        long txId = populatePrototype( prototypeLayout, workLog );

        FileUtils.copyDirectory( prototypeLayout.databaseDirectory(), copyLayout.databaseDirectory() );
        FileUtils.copyDirectory( prototypeLayout.getTransactionLogsDirectory(), copyLayout.getTransactionLogsDirectory() );
        GraphDatabaseAPI db = db( copyLayout );
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            tx.createNode();
            tx.commit();
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
            for ( Operation transaction : workLog.transactions() )
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
        managementService = new TestDatabaseManagementServiceBuilder( neo4jLayout )
                .setConfig( GraphDatabaseSettings.default_database, databaseLayout.getDatabaseName() )
                .build();
        return (GraphDatabaseAPI) managementService.database( databaseLayout.getDatabaseName() );
    }

    enum Operation
    {
        CREATE_NODE
                {
                    @Override
                    void applyTx( GraphDatabaseService graphDb, Transaction tx )
                    {
                        tx.createNode();
                    }
                },
        CREATE_NODE_WITH_PROPERTY
                {
                    @Override
                    void applyTx( GraphDatabaseService graphDb, Transaction tx )
                    {
                        tx.createNode().setProperty( name(), "value" );
                    }
                },
        SET_PROPERTY( CREATE_NODE )
                {
                    @Override
                    void applyTx( GraphDatabaseService graphDb, Transaction tx )
                    {
                        firstNode( tx ).setProperty( name(), "value" );
                    }
                },
        CHANGE_PROPERTY( CREATE_NODE_WITH_PROPERTY )
                {
                    @Override
                    void applyTx( GraphDatabaseService graphDb, Transaction tx )
                    {
                        ResourceIterable<Node> nodes = tx.getAllNodes();
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

        private static Node firstNode( Transaction transaction )
        {
            return Iterables.firstOrNull( transaction.getAllNodes() );
        }

        private final Operation[] dependencies;

        Operation( Operation... dependencies )
        {
            this.dependencies = dependencies;
        }

        void applyTo( GraphDatabaseService graphDb )
        {
            try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
            {
                applyTx( graphDb, tx );

                tx.commit();
            }
        }

        void applyTx( GraphDatabaseService graphDb, Transaction tx )
        {
        }
    }

    static class WorkLog
    {
        static final WorkLog BASE = new WorkLog( EnumSet.noneOf( Operation.class ) );
        final EnumSet<Operation> transactions;

        WorkLog( EnumSet<Operation> transactions )
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

        Operation[] transactions()
        {
            return transactions.toArray( new Operation[0] );
        }

        static Set<WorkLog> combinations()
        {
            Set<WorkLog> combinations = Collections.newSetFromMap( new LinkedHashMap<>() );
            for ( Operation transaction : Operation.values() )
            {
                combinations.add( BASE.extend( transaction ) );
            }
            for ( Operation transaction : Operation.values() )
            {
                for ( WorkLog combination : new ArrayList<>( combinations ) )
                {
                    combinations.add( combination.extend( transaction ) );
                }
            }
            return combinations;
        }

        private WorkLog extend( Operation transaction )
        {
            EnumSet<Operation> base = EnumSet.copyOf( transactions );
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
