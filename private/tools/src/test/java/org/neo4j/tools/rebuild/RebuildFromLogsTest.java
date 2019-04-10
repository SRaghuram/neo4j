/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.rebuild;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Set;

import org.neo4j.consistency.checking.InconsistentStoreException;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

@RunWith( Parameterized.class )
public class RebuildFromLogsTest
{
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final ExpectedException expectedException = ExpectedException.none();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDirectory )
            .around( suppressOutput ).around( fileSystemRule ).around( expectedException );

    private final Transaction[] work;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<WorkLog> commands()
    {
        return WorkLog.combinations();
    }

    @Test
    public void shouldRebuildFromLog() throws Exception, InconsistentStoreException
    {
        // given
        var prototypeLayout = testDirectory.databaseLayout( testDirectory.storeDir( "prototype" ) );
        var rebuildLayout = testDirectory.databaseLayout( testDirectory.storeDir( "rebuild" ) );
        populatePrototype( prototypeLayout );

        // when
        new RebuildFromLogs( fileSystemRule.get() ).rebuild( prototypeLayout, rebuildLayout, BASE_TX_ID );

        // then
        assertEquals( DbRepresentation.of( prototypeLayout ), DbRepresentation.of( rebuildLayout ) );
    }

    @Test
    public void failRebuildFromLogIfStoreIsInconsistentAfterRebuild() throws InconsistentStoreException, Exception
    {
        var prototypeLayout = testDirectory.databaseLayout( "prototype" );
        var rebuildLayout = testDirectory.databaseLayout( "rebuild" );
        populatePrototype( prototypeLayout );

        // when
        expectedException.expect( InconsistentStoreException.class );
        RebuildFromLogs rebuildFromLogs = new TestRebuildFromLogs( fileSystemRule.get() );
        rebuildFromLogs.rebuild( prototypeLayout, rebuildLayout, BASE_TX_ID );
    }

    @Test
    public void shouldRebuildFromLogUpToATx() throws Exception, InconsistentStoreException
    {
        // given
        var prototypeLayout = testDirectory.databaseLayout( "prototype" );
        var copyLayout = testDirectory.databaseLayout( "copy" );
        var rebuildLayout = testDirectory.databaseLayout( "rebuild" );
        long txId = populatePrototype( prototypeLayout );

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
            db.shutdown();
        }

        // when
        new RebuildFromLogs( fileSystemRule.get() ).rebuild( copyLayout, rebuildLayout, txId );

        // then
        assertEquals( DbRepresentation.of( prototypeLayout ), DbRepresentation.of( rebuildLayout ) );
    }

    private long populatePrototype( DatabaseLayout databaseLayout )
    {
        GraphDatabaseAPI prototype = db( databaseLayout );
        long txId;
        try
        {
            for ( Transaction transaction : work )
            {
                transaction.applyTo( prototype );
            }
        }
        finally
        {
            txId = prototype.getDependencyResolver().resolveDependency( MetaDataStore.class ).getLastCommittedTransactionId();
            prototype.shutdown();
        }
        return txId;
    }

    private static GraphDatabaseAPI db( DatabaseLayout databaseLayout )
    {
        DatabaseManagementService managementService = new TestGraphDatabaseFactory().newDatabaseManagementService( databaseLayout.databaseDirectory() );
        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
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

    public RebuildFromLogsTest( WorkLog work )
    {
        this.work = work.transactions();
    }

    private class TestRebuildFromLogs extends RebuildFromLogs
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
