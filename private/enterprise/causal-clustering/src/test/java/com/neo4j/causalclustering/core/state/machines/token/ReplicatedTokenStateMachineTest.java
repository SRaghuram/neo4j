/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.token;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.internal.kernel.api.NamedToken;
import org.neo4j.internal.recordstorage.CacheAccessBackDoor;
import org.neo4j.internal.recordstorage.CacheInvalidationTransactionApplier;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.internal.recordstorage.HighIdTransactionApplier;
import org.neo4j.internal.recordstorage.NeoStoreTransactionApplier;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.core.TokenRegistry;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.SchemaRule;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static com.neo4j.causalclustering.core.state.machines.token.StorageCommandMarshal.commandsToBytes;
import static com.neo4j.causalclustering.core.state.machines.token.TokenType.LABEL;
import static com.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding.decodeLogIndexFromTxHeader;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;
import static org.neo4j.storageengine.api.CommandVersion.AFTER;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

public class ReplicatedTokenStateMachineTest
{
    private final int EXPECTED_TOKEN_ID = 1;
    private final int UNEXPECTED_TOKEN_ID = 1024;
    private final String databaseName = DEFAULT_DATABASE_NAME;

    private TestDirectory testDirectory = TestDirectory.testDirectory();
    private EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private AssertableLogProvider logProvider = new AssertableLogProvider( true );
    private PageCacheRule pageCacheRule = new PageCacheRule();
    private CleanupRule cleanupRule = new CleanupRule();
    @Rule
    public RuleChain rules = RuleChain.outerRule( fs ).around( testDirectory ).around( logProvider ).around( pageCacheRule ).around( cleanupRule );

    @Test
    public void shouldCreateTokenId() throws Exception
    {
        // given
        TokenRegistry registry = new TokenRegistry( "Label" );
        ReplicatedTokenStateMachine stateMachine = new ReplicatedTokenStateMachine( registry,
                NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY );
        stateMachine.installCommitProcess( labelRegistryUpdatingCommitProcess( registry ), -1 );

        // when
        byte[] commandBytes = commandsToBytes( tokenCommands( EXPECTED_TOKEN_ID, false ) );
        stateMachine.applyCommand( new ReplicatedTokenRequest( databaseName, LABEL, "Person", commandBytes ), 1, r -> {} );

        // then
        assertEquals( EXPECTED_TOKEN_ID, (int) registry.getId( "Person" ) );
    }

    @Test
    public void shouldCreateInternalTokenId() throws Exception
    {
        // given
        TokenRegistry registry = new TokenRegistry( "Label" );
        ReplicatedTokenStateMachine stateMachine = new ReplicatedTokenStateMachine( registry,
                NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY );
        stateMachine.installCommitProcess( labelRegistryUpdatingCommitProcess( registry ), -1 );

        // when
        byte[] commandBytes = commandsToBytes( tokenCommands( EXPECTED_TOKEN_ID, true ) );
        stateMachine.applyCommand( new ReplicatedTokenRequest( databaseName, LABEL, "Person", commandBytes ), 1, r -> {} );

        // then
        assertNull( registry.getId( "Person" ) );
        assertEquals( EXPECTED_TOKEN_ID, (int) registry.getIdInternal( "Person" ) );
    }

    @Test
    public void shouldAllocateTokenIdToFirstReplicateRequest() throws Exception
    {
        // given
        TokenRegistry registry = new TokenRegistry( "Label" );
        ReplicatedTokenStateMachine stateMachine = new ReplicatedTokenStateMachine( registry,
                NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY );

        stateMachine.installCommitProcess( labelRegistryUpdatingCommitProcess( registry ), -1 );

        ReplicatedTokenRequest winningRequest =
                new ReplicatedTokenRequest( databaseName, LABEL, "Person", commandsToBytes( tokenCommands( EXPECTED_TOKEN_ID, false ) ) );
        ReplicatedTokenRequest losingRequest =
                new ReplicatedTokenRequest( databaseName, LABEL, "Person", commandsToBytes( tokenCommands( UNEXPECTED_TOKEN_ID, false ) ) );

        // when
        stateMachine.applyCommand( winningRequest, 1, r -> {} );
        stateMachine.applyCommand( losingRequest, 2, r -> {} );

        // then
        assertEquals( EXPECTED_TOKEN_ID, (int) registry.getId( "Person" ) );
    }

    @Test
    public void shouldAllocateInternalTokenIdToFirstReplicateRequest() throws Exception
    {
        // given
        TokenRegistry registry = new TokenRegistry( "Label" );
        ReplicatedTokenStateMachine stateMachine = new ReplicatedTokenStateMachine( registry,
                NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY );

        stateMachine.installCommitProcess( labelRegistryUpdatingCommitProcess( registry ), -1 );

        ReplicatedTokenRequest winningRequest =
                new ReplicatedTokenRequest( databaseName, LABEL, "Person", commandsToBytes( tokenCommands( EXPECTED_TOKEN_ID, true ) ) );
        ReplicatedTokenRequest losingRequest =
                new ReplicatedTokenRequest( databaseName, LABEL, "Person", commandsToBytes( tokenCommands( UNEXPECTED_TOKEN_ID, true ) ) );

        // when
        stateMachine.applyCommand( winningRequest, 1, r -> {} );
        stateMachine.applyCommand( losingRequest, 2, r -> {} );

        // then
        assertNull( registry.getId( "Person" ) );
        assertEquals( EXPECTED_TOKEN_ID, (int) registry.getIdInternal( "Person" ) );
    }

    @Test
    public void shouldStoreRaftLogIndexInTransactionHeader()
    {
        // given
        int logIndex = 1;

        StubTransactionCommitProcess commitProcess = new StubTransactionCommitProcess( null, null );
        ReplicatedTokenStateMachine stateMachine = new ReplicatedTokenStateMachine(
                new TokenRegistry( "Token" ),
                NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY );
        stateMachine.installCommitProcess( commitProcess, -1 );

        // when
        byte[] commandBytes = commandsToBytes( tokenCommands( EXPECTED_TOKEN_ID, false ) );
        stateMachine.applyCommand( new ReplicatedTokenRequest( databaseName, LABEL, "Person", commandBytes ), logIndex, r -> {} );

        // then
        List<TransactionRepresentation> transactions = commitProcess.transactionsToApply;
        assertEquals( 1, transactions.size() );
        assertEquals( logIndex, decodeLogIndexFromTxHeader( transactions.get( 0 ).additionalHeader() ) );
    }

    private static List<StorageCommand> tokenCommands( int expectedTokenId, boolean internal )
    {
        LabelTokenRecord record = new LabelTokenRecord( expectedTokenId ).initialize( true, 7 );
        record.addNameRecord( DynamicRecord.dynamicRecord( 7, true, true, -1, PropertyType.STRING.intValue(), "Person".getBytes( StandardCharsets.UTF_8 ) ) );
        record.setInternal( internal );
        return singletonList( new Command.LabelTokenCommand(
                new LabelTokenRecord( expectedTokenId ), record
        ) );
    }

    private TransactionCommitProcess labelRegistryUpdatingCommitProcess( TokenRegistry registry ) throws Exception
    {
        DatabaseLayout layout = testDirectory.databaseLayout();
        Config config = Config.defaults();
        IdGeneratorFactory idFactory = new DefaultIdGeneratorFactory( fs );
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        StoreFactory storeFactory = new StoreFactory( layout, config, idFactory, pageCache, fs, logProvider );
        NeoStores stores = cleanupRule.add( storeFactory.openAllNeoStores( true ) );
        TransactionCommitProcess commitProcess = mock( TransactionCommitProcess.class );
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
            tta.accept( new NeoStoreTransactionApplier( AFTER, stores, backdoor, NO_LOCK_SERVICE, 13, new LockGroup() ) );
            tta.accept( new CacheInvalidationTransactionApplier( stores, backdoor ) );
            return 13L;
        } );
        return commitProcess;
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
