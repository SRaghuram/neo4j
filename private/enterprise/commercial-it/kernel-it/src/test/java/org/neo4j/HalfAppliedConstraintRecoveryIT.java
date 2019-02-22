/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.Barrier;
import org.neo4j.test.TestLabels;
import org.neo4j.test.rule.OtherThreadRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

/**
 * It's master creating a constraint. There are two mini transactions in creating a constraint:
 * <ol>
 * <li>Create the backing index and activating the constraint (index population follows).</li>
 * <li>Activating the constraint after successful index population.</li>
 * </ol>
 *
 * If slave pulls the first mini transaction, but crashes or otherwise does a nonclean shutdown before it gets
 * the other mini transaction (and that index record happens to have been evicted to disk in between)
 * then the next start of that slave would set that constraint index as failed and even delete it
 * and refuse to activate it when it eventually would pull the other mini transaction which wanted to
 * activate the constraint.
 *
 * This issue is tested in single db mode because it's way easier to reliably test in this environment.
 */
public class HalfAppliedConstraintRecoveryIT
{
    private static final Label LABEL = TestLabels.LABEL_ONE;
    private static final String KEY = "key";
    private static final String KEY2 = "key2";
    private static final Consumer<GraphDatabaseAPI> UNIQUE_CONSTRAINT_CREATOR =
            db -> db.schema().constraintFor( LABEL ).assertPropertyIsUnique( KEY ).create();

    private static final Consumer<GraphDatabaseAPI> NODE_KEY_CONSTRAINT_CREATOR =
            db -> db.execute( "CREATE CONSTRAINT ON (n:" + LABEL.name() + ") ASSERT (n." + KEY + ") IS NODE KEY" );

    private static final Consumer<GraphDatabaseAPI> COMPOSITE_NODE_KEY_CONSTRAINT_CREATOR =
            db -> db.execute( "CREATE CONSTRAINT ON (n:" + LABEL.name() + ") ASSERT (n." + KEY + ", n." + KEY2 + ") IS NODE KEY" );
    private static final BiConsumer<GraphDatabaseAPI,List<TransactionRepresentation>> REAPPLY =
            ( db, txs ) -> apply( db, txs.subList( txs.size() - 1, txs.size() ) );

    private static BiConsumer<GraphDatabaseAPI,List<TransactionRepresentation>> recreate( Consumer<GraphDatabaseAPI> constraintCreator )
    {
        return ( db, txs ) -> createConstraint( db, constraintCreator );
    }

    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    @Rule
    public final OtherThreadRule<Void> t2 = new OtherThreadRule<>( "T2" );
    private final Monitors monitors = new Monitors();

    @Test
    public void recoverFromAndContinueApplyHalfConstraintAppliedBeforeCrash() throws Exception
    {
        recoverFromHalfConstraintAppliedBeforeCrash( REAPPLY, UNIQUE_CONSTRAINT_CREATOR, false );
    }

    @Test
    public void recoverFromAndRecreateHalfConstraintAppliedBeforeCrash() throws Exception
    {
        recoverFromHalfConstraintAppliedBeforeCrash( recreate( UNIQUE_CONSTRAINT_CREATOR ), UNIQUE_CONSTRAINT_CREATOR, false );
    }

    @Test
    public void recoverFromAndContinueApplyHalfNodeKeyConstraintAppliedBeforeCrash() throws Exception
    {
        recoverFromHalfConstraintAppliedBeforeCrash( REAPPLY, NODE_KEY_CONSTRAINT_CREATOR, false );
    }

    @Test
    public void recoverFromAndRecreateHalfNodeKeyConstraintAppliedBeforeCrash() throws Exception
    {
        recoverFromHalfConstraintAppliedBeforeCrash( recreate( NODE_KEY_CONSTRAINT_CREATOR ), NODE_KEY_CONSTRAINT_CREATOR, false );
    }

    @Test
    public void recoverFromAndContinueApplyHalfCompositeNodeKeyConstraintAppliedBeforeCrash() throws Exception
    {
        recoverFromHalfConstraintAppliedBeforeCrash( REAPPLY, COMPOSITE_NODE_KEY_CONSTRAINT_CREATOR, true );
    }

    @Test
    public void recoverFromAndRecreateHalfCompositeNodeKeyConstraintAppliedBeforeCrash() throws Exception
    {
        recoverFromHalfConstraintAppliedBeforeCrash( recreate( COMPOSITE_NODE_KEY_CONSTRAINT_CREATOR ), COMPOSITE_NODE_KEY_CONSTRAINT_CREATOR, true );
    }

    private void recoverFromHalfConstraintAppliedBeforeCrash( BiConsumer<GraphDatabaseAPI,List<TransactionRepresentation>> applier,
            Consumer<GraphDatabaseAPI> constraintCreator, boolean composite ) throws Exception
    {
        // GIVEN
        List<TransactionRepresentation> transactions = createTransactionsForCreatingConstraint( constraintCreator );
        GraphDatabaseAPI db = newDb();
        EphemeralFileSystemAbstraction crashSnapshot;
        try
        {
            apply( db, transactions.subList( 0, transactions.size() - 1 ) );
            flushStores( db );
            crashSnapshot = fs.snapshot();
        }
        finally
        {
            db.shutdown();
        }

        // WHEN
        db = (GraphDatabaseAPI) new TestCommercialGraphDatabaseFactory().setFileSystem( crashSnapshot ).newImpermanentDatabase();
        try
        {
            applier.accept( db, transactions );

            // THEN
            try ( Transaction tx = db.beginTx() )
            {
                ConstraintDefinition constraint = single( db.schema().getConstraints( LABEL ) );
                assertEquals( LABEL.name(), constraint.getLabel().name() );
                if ( composite )
                {
                    assertEquals( Arrays.asList( KEY, KEY2 ), Iterables.asList( constraint.getPropertyKeys() ) );
                }
                else
                {
                    assertEquals( KEY, single( constraint.getPropertyKeys() ) );
                }
                IndexDefinition index = single( db.schema().getIndexes( LABEL ) );
                assertEquals( LABEL.name(), single( index.getLabels() ).name() );
                if ( composite )
                {
                    assertEquals( Arrays.asList( KEY, KEY2 ), Iterables.asList( index.getPropertyKeys() ) );
                }
                else
                {
                    assertEquals( KEY, single( index.getPropertyKeys() ) );
                }
                tx.success();
            }
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void recoverFromNonUniqueHalfConstraintAppliedBeforeCrash() throws Exception
    {
        // GIVEN
        recoverFromConstraintAppliedBeforeCrash( UNIQUE_CONSTRAINT_CREATOR );
    }

    @Test
    public void recoverFromNonUniqueHalfNodeKeyConstraintAppliedBeforeCrash() throws Exception
    {
        // GIVEN
        recoverFromConstraintAppliedBeforeCrash( NODE_KEY_CONSTRAINT_CREATOR );
    }

    @Test
    public void recoverFromNonUniqueHalfCompositeNodeKeyConstraintAppliedBeforeCrash() throws Exception
    {
        // GIVEN
        recoverFromConstraintAppliedBeforeCrash( COMPOSITE_NODE_KEY_CONSTRAINT_CREATOR );
    }

    private void recoverFromConstraintAppliedBeforeCrash( Consumer<GraphDatabaseAPI> constraintCreator ) throws Exception
    {
        List<TransactionRepresentation> transactions = createTransactionsForCreatingConstraint( constraintCreator );
        EphemeralFileSystemAbstraction crashSnapshot;
        {
            GraphDatabaseAPI db = newDb();
            Barrier.Control barrier = new Barrier.Control();
            monitors.addMonitorListener( new IndexingService.MonitorAdapter()
            {
                @Override
                public void indexPopulationScanStarting()
                {
                    barrier.reached();
                }
            } );
            try
            {
                // Create two nodes that have duplicate property values
                createData( db, "v", "v" );
                t2.execute( state ->
                {
                    List<TransactionRepresentation> transactionsButWithoutConstraintCreation = transactions.subList( 0, transactions.size() - 1 );
                    apply( db, transactionsButWithoutConstraintCreation );
                    return null;
                } );
                barrier.await();
                flushStores( db );
                // Crash before index population have discovered that there are duplicates, right before scan actually starts.
                crashSnapshot = fs.snapshot();
                barrier.release();
            }
            finally
            {
                db.shutdown();
            }
        }

        // WHEN
        {
            GraphDatabaseAPI db = (GraphDatabaseAPI) new TestCommercialGraphDatabaseFactory().setFileSystem( crashSnapshot )
                    .newImpermanentDatabase();
            try
            {
                recreate( constraintCreator ).accept( db, transactions );
                fail( "Should not be able to create constraint on non-unique data" );
            }
            catch ( ConstraintViolationException | QueryExecutionException e )
            {
                // THEN good
            }
            finally
            {
                db.shutdown();
            }
        }
    }

    private static void createData( GraphDatabaseAPI db, String... values )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( String value : values )
            {
                Node node = db.createNode( LABEL );
                node.setProperty( KEY, value );
                node.setProperty( KEY2, value );
            }
            tx.success();
        }
    }

    private GraphDatabaseAPI newDb()
    {
        return (GraphDatabaseAPI) new TestCommercialGraphDatabaseFactory().setFileSystem( fs ).setMonitors( monitors )
                .newImpermanentDatabase();
    }

    private static void flushStores( GraphDatabaseAPI db ) throws IOException
    {
        db.getDependencyResolver().resolveDependency( RecordStorageEngine.class )
                .testAccessNeoStores().flush( IOLimiter.UNLIMITED );
    }

    private static void apply( GraphDatabaseAPI db, List<TransactionRepresentation> transactions )
    {
        TransactionCommitProcess committer =
                db.getDependencyResolver().resolveDependency( TransactionCommitProcess.class );
        transactions.forEach( tx ->
        {
            try
            {
                committer.commit( new TransactionToApply( tx ), CommitEvent.NULL, EXTERNAL );
            }
            catch ( TransactionFailureException e )
            {
                throw new RuntimeException( e );
            }
        } );
    }

    private List<TransactionRepresentation> createTransactionsForCreatingConstraint( Consumer<GraphDatabaseAPI> uniqueConstraintCreator )
            throws Exception
    {
        // A separate db altogether
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestCommercialGraphDatabaseFactory().newImpermanentDatabase();
        try
        {
            LogicalTransactionStore txStore =
                    db.getDependencyResolver().resolveDependency( LogicalTransactionStore.class );
            createData( db, "a", "b" );
            List<TransactionRepresentation> initialTransactions = extractTransactions( txStore );
            createConstraint( db, uniqueConstraintCreator );
            List<TransactionRepresentation> transactions = extractTransactions( txStore );
            for ( TransactionRepresentation initialTransaction : initialTransactions )
            {
                // Preserve the token create transactions, but remove the node create and property set transactions.
                if ( initialTransaction.accept( containNodeCommand() ) )
                {
                    assertTrue( transactions.remove( initialTransaction ) );
                }
            }
            return transactions;
        }
        finally
        {
            db.shutdown();
        }
    }

    private static Visitor<StorageCommand,IOException> containNodeCommand()
    {
        return element -> element instanceof Command.NodeCommand;
    }

    private static List<TransactionRepresentation> extractTransactions( LogicalTransactionStore txStore ) throws IOException
    {
        List<TransactionRepresentation> transactions = new ArrayList<>();
        try ( TransactionCursor cursor = txStore.getTransactions( TransactionIdStore.BASE_TX_ID + 1 ) )
        {
            cursor.forAll( tx -> transactions.add( tx.getTransactionRepresentation() ) );
        }
        return transactions;
    }

    private static void createConstraint( GraphDatabaseAPI db, Consumer<GraphDatabaseAPI> constraintCreator )
    {
        try ( Transaction tx = db.beginTx() )
        {
            constraintCreator.accept( db );
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.HOURS );
            tx.success();
        }
    }
}
