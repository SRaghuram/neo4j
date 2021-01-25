/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.applytx;

import com.neo4j.tools.input.ArgsCommand;

import java.io.PrintStream;
import java.util.function.Supplier;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.cursor.IOCursor;
import org.neo4j.internal.helpers.Args;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.api.InternalTransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionIdStore;

import static java.lang.System.lineSeparator;
import static org.neo4j.internal.helpers.progress.ProgressMonitorFactory.textual;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static org.neo4j.kernel.impl.transaction.tracing.CommitEvent.NULL;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

/**
 * Applies one or more transactions read from a separate transaction log onto the the store.
 * It's only the transaction contents that are being applied and so theoretically the transaction ids in the target store
 * may not match those from the source, but practically this is what usually happens because the transaction range usually
 * starts from the beginning.
 */
public class ApplyTransactionsCommand extends ArgsCommand
{
    private final DatabaseLayout fromLayout;
    private final CommandReaderFactory fromCommandReader;
    private final Supplier<GraphDatabaseAPI> to;

    ApplyTransactionsCommand( DatabaseLayout fromLayout, CommandReaderFactory fromCommandReader, Supplier<GraphDatabaseAPI> to )
    {
        this.fromLayout = fromLayout;
        this.fromCommandReader = fromCommandReader;
        this.to = to;
    }

    @Override
    protected void run( Args args, PrintStream out ) throws Exception
    {
        DependencyResolver dependencyResolver = to.get().getDependencyResolver();
        TransactionIdStore txIdStore = dependencyResolver.resolveDependency( TransactionIdStore.class );
        long fromTx = txIdStore.getLastCommittedTransaction().transactionId();
        long toTx;
        if ( args.orphans().isEmpty() )
        {
            throw new IllegalArgumentException( "No tx specified" );
        }

        String whereTo = args.orphans().get( 0 );
        if ( whereTo.equals( "next" ) )
        {
            toTx = fromTx + 1;
        }
        else if ( whereTo.equals( "last" ) )
        {
            toTx = Long.MAX_VALUE;
        }
        else
        {
            toTx = Long.parseLong( whereTo );
        }

        long lastApplied = applyTransactions( fromLayout, to.get(), fromTx, toTx, out );
        out.println( "Applied transactions up to and including " + lastApplied );
    }

    private long applyTransactions( DatabaseLayout fromLayout, GraphDatabaseAPI toDb,
            long fromTxExclusive, long toTxInclusive, PrintStream out ) throws Exception
    {
        DependencyResolver resolver = toDb.getDependencyResolver();
        InternalTransactionCommitProcess commitProcess =
                new InternalTransactionCommitProcess(
                        resolver.resolveDependency( TransactionAppender.class ),
                        resolver.resolveDependency( StorageEngine.class ) );
        LifeSupport life = new LifeSupport();
        try ( DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              JobScheduler jobScheduler = createInitialisedScheduler();
              PageCache pageCache = StandalonePageCacheFactory.createPageCache( fileSystem, jobScheduler, PageCacheTracer.NULL ) )
        {
            LogicalTransactionStore source =
                    life.add( new ReadOnlyTransactionStore( pageCache, fileSystem, fromLayout, Config.defaults(), new Monitors(), fromCommandReader ) );
            life.start();
            long lastAppliedTx = fromTxExclusive;
            // Some progress if there are more than a couple of transactions to apply
            ProgressListener progress = toTxInclusive - fromTxExclusive >= 100 ?
                                        textual( out ).singlePart( "Application progress",
                                                toTxInclusive - fromTxExclusive ) :
                                        ProgressListener.NONE;
            try ( IOCursor<CommittedTransactionRepresentation> cursor = source.getTransactions( fromTxExclusive + 1 ) )
            {
                while ( cursor.next() )
                {
                    CommittedTransactionRepresentation transaction = cursor.get();
                    TransactionRepresentation transactionRepresentation =
                            transaction.getTransactionRepresentation();
                    try
                    {
                        commitProcess.commit( new TransactionToApply( transactionRepresentation, PageCursorTracer.NULL ), NULL, EXTERNAL );
                        progress.add( 1 );
                    }
                    catch ( final Throwable e )
                    {
                        System.err.println( "ERROR applying transaction " + transaction.getCommitEntry().getTxId() );
                        throw e;
                    }
                    lastAppliedTx = transaction.getCommitEntry().getTxId();
                    if ( lastAppliedTx == toTxInclusive )
                    {
                        break;
                    }
                }
            }
            return lastAppliedTx;
        }
        finally
        {
            life.shutdown();
        }
    }

    @Override
    public String toString()
    {
        return String.join( lineSeparator(),
                "Applies transactions from the source onto the new db. Example:",
                "  apply last : applies transactions from the currently last applied and up to the last",
                "               transaction of source db",
                "  apply next : applies the next transaction onto the new db",
                "  apply 234  : applies up to and including tx 234 from the source db onto the new db" );
    }
}
