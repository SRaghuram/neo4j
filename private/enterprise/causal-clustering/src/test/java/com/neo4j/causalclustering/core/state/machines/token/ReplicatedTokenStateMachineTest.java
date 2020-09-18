/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.token;

import com.neo4j.causalclustering.core.state.machines.DummyStateMachineCommitHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.BatchContext;
import org.neo4j.internal.recordstorage.CacheAccessBackDoor;
import org.neo4j.internal.recordstorage.CacheInvalidationTransactionApplier;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.internal.recordstorage.HighIdTransactionApplier;
import org.neo4j.internal.recordstorage.NeoStoreTransactionApplier;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.store.IdUpdateListener;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.token.TokenRegistry;
import org.neo4j.token.api.NamedToken;

import static com.neo4j.causalclustering.core.state.machines.token.StorageCommandMarshal.commandsToBytes;
import static com.neo4j.causalclustering.core.state.machines.token.TokenType.LABEL;
import static com.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding.decodeLogIndexFromTxHeader;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.recordstorage.RecordStorageCommandReaderFactory.INSTANCE;
import static org.neo4j.lock.LockService.NO_LOCK_SERVICE;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;
import static org.neo4j.storageengine.api.CommandVersion.AFTER;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectorySupportExtension.class, PageCacheSupportExtension.class} )
class ReplicatedTokenStateMachineTest
{
    private final int EXPECTED_TOKEN_ID = 1;
    private final int UNEXPECTED_TOKEN_ID = 1024;
    private final DatabaseId databaseId = TestDatabaseIdRepository.randomNamedDatabaseId().databaseId();
    private final AssertableLogProvider logProvider = new AssertableLogProvider( true );
    private final LogEntryWriterFactory logEntryWriterFactory = LogEntryWriterFactory.LATEST;

    private NeoStores stores;

    @Inject
    private TestDirectory testDirectory;
    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private PageCache pageCache;

    @AfterEach
    void closeStores()
    {
        if ( stores != null )
        {
            stores.close();
            stores = null;
        }
    }

    @Test
    void shouldCreateTokenId() throws Exception
    {
        // given
        var registry = new TokenRegistry( "Label" );
        var stateMachine = newTokenStateMachine( registry );
        stateMachine.installCommitProcess( labelRegistryUpdatingCommitProcess( registry ), -1 );

        // when
        var commandBytes = commandsToBytes( tokenCommands( EXPECTED_TOKEN_ID, false ), logEntryWriterFactory );
        stateMachine.applyCommand( new ReplicatedTokenRequest( databaseId, LABEL, "Person", commandBytes ), 1, r -> {} );

        // then
        assertEquals( EXPECTED_TOKEN_ID, (int) registry.getId( "Person" ) );
    }

    @Test
    void shouldCreateInternalTokenId() throws Exception
    {
        // given
        var registry = new TokenRegistry( "Label" );
        var stateMachine = newTokenStateMachine( registry );
        stateMachine.installCommitProcess( labelRegistryUpdatingCommitProcess( registry ), -1 );

        // when
        var commandBytes = commandsToBytes( tokenCommands( EXPECTED_TOKEN_ID, true ), logEntryWriterFactory );
        stateMachine.applyCommand( new ReplicatedTokenRequest( databaseId, LABEL, "Person", commandBytes ), 1, r -> {} );

        // then
        assertNull( registry.getId( "Person" ) );
        assertEquals( EXPECTED_TOKEN_ID, (int) registry.getIdInternal( "Person" ) );
    }

    @Test
    void shouldAllocateTokenIdToFirstReplicateRequest() throws Exception
    {
        // given
        var registry = new TokenRegistry( "Label" );
        var stateMachine = newTokenStateMachine( registry );

        stateMachine.installCommitProcess( labelRegistryUpdatingCommitProcess( registry ), -1 );

        var winningRequest =
                new ReplicatedTokenRequest( databaseId, LABEL, "Person",
                                            commandsToBytes( tokenCommands( EXPECTED_TOKEN_ID, false ), logEntryWriterFactory ) );
        var losingRequest =
                new ReplicatedTokenRequest( databaseId, LABEL, "Person",
                                            commandsToBytes( tokenCommands( UNEXPECTED_TOKEN_ID, false ), logEntryWriterFactory ) );

        // when
        stateMachine.applyCommand( winningRequest, 1, r -> {} );
        stateMachine.applyCommand( losingRequest, 2, r -> {} );

        // then
        assertEquals( EXPECTED_TOKEN_ID, (int) registry.getId( "Person" ) );
    }

    @Test
    void shouldAllocateInternalTokenIdToFirstReplicateRequest() throws Exception
    {
        // given
        var registry = new TokenRegistry( "Label" );
        var stateMachine = newTokenStateMachine( registry );

        stateMachine.installCommitProcess( labelRegistryUpdatingCommitProcess( registry ), -1 );

        var winningRequest =
                new ReplicatedTokenRequest( databaseId, LABEL, "Person",
                                            commandsToBytes( tokenCommands( EXPECTED_TOKEN_ID, true ), logEntryWriterFactory ) );
        var losingRequest =
                new ReplicatedTokenRequest( databaseId, LABEL, "Person",
                                            commandsToBytes( tokenCommands( UNEXPECTED_TOKEN_ID, true ), logEntryWriterFactory ) );

        // when
        stateMachine.applyCommand( winningRequest, 1, r -> {} );
        stateMachine.applyCommand( losingRequest, 2, r -> {} );

        // then
        assertNull( registry.getId( "Person" ) );
        assertEquals( EXPECTED_TOKEN_ID, (int) registry.getIdInternal( "Person" ) );
    }

    @Test
    void shouldStoreRaftLogIndexInTransactionHeader()
    {
        // given
        var logIndex = 1;

        var commitProcess = new StubTransactionCommitProcess( null, null );
        var stateMachine = newTokenStateMachine( new TokenRegistry( "Token" ) );
        stateMachine.installCommitProcess( commitProcess, -1 );

        // when
        var commandBytes = commandsToBytes( tokenCommands( EXPECTED_TOKEN_ID, false ), logEntryWriterFactory );
        stateMachine.applyCommand( new ReplicatedTokenRequest( databaseId, LABEL, "Person", commandBytes ), logIndex, r -> {} );

        // then
        var transactions = commitProcess.transactionsToApply;
        assertEquals( 1, transactions.size() );
        assertEquals( logIndex, decodeLogIndexFromTxHeader( transactions.get( 0 ).additionalHeader() ) );
    }

    private static List<StorageCommand> tokenCommands( int expectedTokenId, boolean internal )
    {
        var record = new LabelTokenRecord( expectedTokenId ).initialize( true, 7 );
        var dynamicRecord = new DynamicRecord( 7 ).initialize( true, true, -1, PropertyType.STRING.intValue() );
        dynamicRecord.setData( "Person".getBytes( StandardCharsets.UTF_8 )  );
        record.addNameRecord( dynamicRecord );
        record.setInternal( internal );
        return singletonList( new Command.LabelTokenCommand(
                new LabelTokenRecord( expectedTokenId ), record
        ) );
    }

    private TransactionCommitProcess labelRegistryUpdatingCommitProcess( TokenRegistry registry ) throws Exception
    {
        var layout = DatabaseLayout.ofFlat( testDirectory.homePath( GraphDatabaseSettings.DEFAULT_DATABASE_NAME ) );
        var config = Config.defaults();
        var idFactory = new DefaultIdGeneratorFactory( fs, immediate() );
        var storeFactory = new StoreFactory( layout, config, idFactory, pageCache, fs, logProvider, PageCacheTracer.NULL );
        assertNull( stores );
        stores = storeFactory.openAllNeoStores( true );
        var commitProcess = mock( TransactionCommitProcess.class );
        when( commitProcess.commit( any( TransactionToApply.class ), any( CommitEvent.class ), eq( EXTERNAL ) ) ).then( inv ->
        {
            TransactionToApply tta = inv.getArgument( 0 );
            CacheAccessBackDoor backdoor = new CacheAccessBackDoor()
            {
                @Override
                public void addSchemaRule( SchemaRule schemaRule )
                {
                }

                @Override
                public void removeSchemaRuleFromCache( long id )
                {
                }

                @Override
                public void addRelationshipTypeToken( NamedToken type )
                {
                }

                @Override
                public void addLabelToken( NamedToken labelId )
                {
                    registry.put( labelId );
                }

                @Override
                public void addPropertyKeyToken( NamedToken index )
                {
                }
            };
            tta.accept( new HighIdTransactionApplier( stores ) );
            var batchContext = mock( BatchContext.class );
            when( batchContext.getIdUpdateListener() ).thenReturn( IdUpdateListener.DIRECT );
            tta.accept( new NeoStoreTransactionApplier( AFTER, stores, backdoor, NO_LOCK_SERVICE, 13, batchContext,
                    PageCursorTracer.NULL ) );
            tta.accept( new CacheInvalidationTransactionApplier( stores, backdoor, PageCursorTracer.NULL ) );
            return 13L;
        } );
        return commitProcess;
    }

    private static ReplicatedTokenStateMachine newTokenStateMachine( TokenRegistry tokenRegistry )
    {
        return new ReplicatedTokenStateMachine( new DummyStateMachineCommitHelper(), tokenRegistry, nullLogProvider(), INSTANCE );
    }

    private static class StubTransactionCommitProcess extends TransactionRepresentationCommitProcess
    {
        private final List<TransactionRepresentation> transactionsToApply = new ArrayList<>();

        StubTransactionCommitProcess( TransactionAppender appender, StorageEngine storageEngine )
        {
            super( appender, storageEngine );
        }

        @Override
        public long commit( TransactionToApply batch, CommitEvent commitEvent, TransactionApplicationMode mode )
                throws TransactionFailureException
        {
            transactionsToApply.add( batch.transactionRepresentation() );
            return -1;
        }
    }
}
