/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.Barrier;
import org.neo4j.test.TestLabels;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.OtherThreadRule;
import org.neo4j.token.api.NonUniqueTokenException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

/**
 * It's master creating a constraint. There are two mini transactions in creating a constraint:
 * <ol>
 * <li>Create the backing index and activating the constraint (index population follows).</li>
 * <li>Activating the constraint after successful index population.</li>
 * </ol>
 * <p>
 * If slave pulls the first mini transaction, but crashes or otherwise does a nonclean shutdown before it gets
 * the other mini transaction (and that index record happens to have been evicted to disk in between)
 * then the next start of that slave would set that constraint index as failed and even delete it
 * and refuse to activate it when it eventually would pull the other mini transaction which wanted to
 * activate the constraint.
 * <p>
 * This issue is tested in single db mode because it's way easier to reliably test in this environment.
 */
@EphemeralTestDirectoryExtension
@ExtendWith( OtherThreadExtension.class )
public class HalfAppliedConstraintRecoveryIT
{
    private static final String NAME = "MyConstraint";
    private static final Label LABEL = TestLabels.LABEL_ONE;
    private static final String KEY = "key";
    private static final String KEY2 = "key2";

    private static final BiConsumer<GraphDatabaseAPI,Transaction> UNIQUE_CONSTRAINT_CREATOR =
            ( db, tx ) -> tx.schema().constraintFor( LABEL ).assertPropertyIsUnique( KEY ).withName( NAME ).create();

    private static final BiConsumer<GraphDatabaseAPI,Transaction> NODE_KEY_CONSTRAINT_CREATOR =
            ( db, tx ) -> tx.schema().constraintFor( LABEL ).assertPropertyIsNodeKey( KEY ).withName( NAME ).create();

    private static final BiConsumer<GraphDatabaseAPI,Transaction> COMPOSITE_NODE_KEY_CONSTRAINT_CREATOR =
            ( db, tx ) -> tx.schema().constraintFor( LABEL ).assertPropertyIsNodeKey( KEY ).assertPropertyIsNodeKey( KEY2 ).withName( NAME ).create();

    private static final BiConsumer<GraphDatabaseAPI,List<TransactionRepresentation>> REAPPLY =
            ( db, txs ) -> apply( db, txs.subList( txs.size() - 1, txs.size() ) );

    private static BiConsumer<GraphDatabaseAPI,List<TransactionRepresentation>> recreate( BiConsumer<GraphDatabaseAPI,Transaction> constraintCreator )
    {
        return ( db, txs ) -> createConstraint( db, constraintCreator );
    }

    /**
     * Creating a constraint will not adopt any orphaned constraint indexes that happens to match their schema.
     * Instead an AlreadyIndexesException will be thrown, and it is up to the human operator to drop those orphaned indexes.
     * This method injects that index-drop operation to simulate that behaviour.
     */
    private static BiConsumer<GraphDatabaseAPI,Transaction> dropIndexAnd( BiConsumer<GraphDatabaseAPI,Transaction> createConstraint )
    {
        return ( db, tx ) ->
        {
            IndexDefinition index = tx.schema().getIndexByName( NAME );
            index.drop();
            createConstraint.accept( db, tx );
        };
    }

    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private OtherThreadRule t2;
    private DatabaseManagementService managementService;
    private final AssertableLogProvider logProvider = new AssertableLogProvider( true );
    private final Monitors monitors = new Monitors();

    @Test
    void recoverFromAndContinueApplyHalfConstraintAppliedBeforeCrash() throws Exception
    {
        recoverFromHalfConstraintAppliedBeforeCrash( REAPPLY, UNIQUE_CONSTRAINT_CREATOR, false );
    }

    @Test
    void recoverFromAndRecreateHalfConstraintAppliedBeforeCrash() throws Exception
    {
        recoverFromHalfConstraintAppliedBeforeCrash( recreate( dropIndexAnd( UNIQUE_CONSTRAINT_CREATOR ) ), UNIQUE_CONSTRAINT_CREATOR, false );
    }

    @Test
    void recoverFromAndContinueApplyHalfNodeKeyConstraintAppliedBeforeCrash() throws Exception
    {
        recoverFromHalfConstraintAppliedBeforeCrash( REAPPLY, NODE_KEY_CONSTRAINT_CREATOR, false );
    }

    @Test
    void recoverFromAndRecreateHalfNodeKeyConstraintAppliedBeforeCrash() throws Exception
    {
        recoverFromHalfConstraintAppliedBeforeCrash( recreate( dropIndexAnd( NODE_KEY_CONSTRAINT_CREATOR ) ), NODE_KEY_CONSTRAINT_CREATOR, false );
    }

    @Test
    void recoverFromAndContinueApplyHalfCompositeNodeKeyConstraintAppliedBeforeCrash() throws Exception
    {
        recoverFromHalfConstraintAppliedBeforeCrash( REAPPLY, COMPOSITE_NODE_KEY_CONSTRAINT_CREATOR, true );
    }

    @Test
    void recoverFromAndRecreateHalfCompositeNodeKeyConstraintAppliedBeforeCrash() throws Exception
    {
        recoverFromHalfConstraintAppliedBeforeCrash( recreate( dropIndexAnd( COMPOSITE_NODE_KEY_CONSTRAINT_CREATOR ) ), COMPOSITE_NODE_KEY_CONSTRAINT_CREATOR,
                true );
    }

    private void recoverFromHalfConstraintAppliedBeforeCrash( BiConsumer<GraphDatabaseAPI,List<TransactionRepresentation>> applier,
            BiConsumer<GraphDatabaseAPI,Transaction> constraintCreator, boolean composite ) throws Exception
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
            managementService.shutdown();
        }

        // WHEN
        TestEnterpriseDatabaseManagementServiceBuilder builder =
                new TestEnterpriseDatabaseManagementServiceBuilder().setFileSystem( crashSnapshot ).impermanent().setInternalLogProvider(
                        logProvider ).setUserLogProvider( logProvider );
        DatabaseManagementService managementService = builder.build();
        db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            applier.accept( db, transactions );

            // THEN
            try ( Transaction tx = db.beginTx() )
            {
                ConstraintDefinition constraint = single( tx.schema().getConstraints( LABEL ) );
                assertEquals( LABEL.name(), constraint.getLabel().name() );
                if ( composite )
                {
                    assertEquals( Arrays.asList( KEY, KEY2 ), Iterables.asList( constraint.getPropertyKeys() ) );
                }
                else
                {
                    assertEquals( KEY, single( constraint.getPropertyKeys() ) );
                }
                IndexDefinition index = single( tx.schema().getIndexes( LABEL ) );
                assertEquals( LABEL.name(), single( index.getLabels() ).name() );
                if ( composite )
                {
                    assertEquals( Arrays.asList( KEY, KEY2 ), Iterables.asList( index.getPropertyKeys() ) );
                }
                else
                {
                    assertEquals( KEY, single( index.getPropertyKeys() ) );
                }
                tx.commit();
            }
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void recoverFromNonUniqueHalfConstraintAppliedBeforeCrash() throws Exception
    {
        // GIVEN
        recoverFromConstraintAppliedBeforeCrash( UNIQUE_CONSTRAINT_CREATOR );
    }

    @Test
    void recoverFromNonUniqueHalfNodeKeyConstraintAppliedBeforeCrash() throws Exception
    {
        // GIVEN
        recoverFromConstraintAppliedBeforeCrash( NODE_KEY_CONSTRAINT_CREATOR );
    }

    @Test
    void recoverFromNonUniqueHalfCompositeNodeKeyConstraintAppliedBeforeCrash() throws Exception
    {
        // GIVEN
        recoverFromConstraintAppliedBeforeCrash( COMPOSITE_NODE_KEY_CONSTRAINT_CREATOR );
    }

    private void recoverFromConstraintAppliedBeforeCrash( BiConsumer<GraphDatabaseAPI,Transaction> constraintCreator ) throws Exception
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
                t2.execute( () ->
                {

                    int ordinaryTokenCreateTransactionsToRemove = countInitialTokenCreatesToRemove( transactions );
                    List<TransactionRepresentation> transactionsButWithoutConstraintCreation =
                            transactions.subList( ordinaryTokenCreateTransactionsToRemove, transactions.size() - 1 );
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
                managementService.shutdown();
            }
        }

        // WHEN
        {
            DatabaseManagementService managementService =
                    new TestEnterpriseDatabaseManagementServiceBuilder().setFileSystem( crashSnapshot ).impermanent().build();
            try
            {
                GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
                var e = assertThrows( Exception.class, () -> recreate( constraintCreator ).accept( db, transactions ) );
                assertThat( e ).isInstanceOfAny( ConstraintViolationException.class, QueryExecutionException.class );
            }
            finally
            {
                managementService.shutdown();
            }
        }
    }

    /**
     * Token create commands cannot be applied more than once, since it would fail with {@link NonUniqueTokenException}s, so we have to filter those
     * create commands out, since the prior call to {@link #createData(GraphDatabaseAPI, String...)} would have caused the tokens to be created.
     * <p>
     * We do this filtering by looking at the "internal" flag of the tokens. This is somewhat hacky, but for the time being we are able to rely on the
     * fact that only the schema store uses internal tokens, so in this test we will only see them immediately before the schema commands themselves.
     */
    private int countInitialTokenCreatesToRemove( List<TransactionRepresentation> transactions )
    {
        int count = 0;
        for ( TransactionRepresentation transaction : transactions )
        {
            for ( StorageCommand command : transaction )
            {
                if ( command instanceof StorageCommand.TokenCommand )
                {
                    StorageCommand.TokenCommand tokenCommand = (StorageCommand.TokenCommand) command;
                    if ( !tokenCommand.isInternal() )
                    {
                        count++;
                    }
                    else
                    {
                        break;
                    }
                }
                else
                {
                    break;
                }
            }
        }
        return count;
    }

    private static void createData( GraphDatabaseAPI db, String... values )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( String value : values )
            {
                Node node = tx.createNode( LABEL );
                node.setProperty( KEY, value );
                node.setProperty( KEY2, value );
            }
            tx.commit();
        }
    }

    private GraphDatabaseAPI newDb()
    {
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder().setFileSystem( fs ).setMonitors( monitors ).impermanent().build();
        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    private static void flushStores( GraphDatabaseAPI db ) throws IOException
    {
        db.getDependencyResolver().resolveDependency( RecordStorageEngine.class ).testAccessNeoStores().flush( IOLimiter.UNLIMITED, NULL );
    }

    private static void apply( GraphDatabaseAPI db, List<TransactionRepresentation> transactions )
    {
        TransactionCommitProcess committer = db.getDependencyResolver().resolveDependency( TransactionCommitProcess.class );
        transactions.forEach( tx ->
        {
            try
            {
                committer.commit( new TransactionToApply( tx, NULL ), CommitEvent.NULL, EXTERNAL );
            }
            catch ( TransactionFailureException e )
            {
                throw new RuntimeException( e );
            }
        } );
    }

    private List<TransactionRepresentation> createTransactionsForCreatingConstraint( BiConsumer<GraphDatabaseAPI,Transaction> uniqueConstraintCreator )
            throws Exception
    {
        // A separate db altogether
        DatabaseManagementService managementService = new TestEnterpriseDatabaseManagementServiceBuilder().impermanent().build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            LogicalTransactionStore txStore = db.getDependencyResolver().resolveDependency( LogicalTransactionStore.class );
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
            managementService.shutdown();
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

    private static void createConstraint( GraphDatabaseAPI db, BiConsumer<GraphDatabaseAPI,Transaction> constraintCreator )
    {
        try ( Transaction tx = db.beginTx() )
        {
            constraintCreator.accept( db, tx );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.HOURS );
            tx.commit();
        }
    }
}
