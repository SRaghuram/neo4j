/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.token;

import com.neo4j.causalclustering.core.replication.ReplicationResult;
import com.neo4j.causalclustering.core.replication.Replicator;
import com.neo4j.causalclustering.core.state.StateMachineResult;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.function.Supplier;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.token.TokenRegistry;
import org.neo4j.token.api.NamedToken;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Answers.RETURNS_MOCKS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

class ReplicatedTokenHolderTest
{
    private StorageEngine storageEngine;
    private final Supplier<StorageEngine> storageEngineSupplier = () -> storageEngine;
    private final NamedDatabaseId namedDatabaseId = new TestDatabaseIdRepository().defaultDatabase();
    private final LogEntryWriterFactory logEntryWriterFactory = LogEntryWriterFactory.LATEST;

    @Test
    void shouldStoreInitialTokens()
    {
        // given
        TokenRegistry registry = new TokenRegistry( "Label" );
        ReplicatedTokenHolder tokenHolder = new ReplicatedLabelTokenHolder( namedDatabaseId, registry, null, null, storageEngineSupplier,
                PageCacheTracer.NULL, INSTANCE, logEntryWriterFactory );

        // when
        tokenHolder.setInitialTokens( asList( new NamedToken( "name1", 1 ), new NamedToken( "name2", 2 ) ) );

        // then
        assertThat( tokenHolder.getAllTokens(), hasItems( new NamedToken( "name1", 1 ), new NamedToken( "name2", 2 ) ) );
    }

    @Test
    void tracePageCacheAccessInReplicatedTokenHolder() throws Exception
    {
        storageEngine = mockedStorageEngine();
        var registry = new TokenRegistry( "Label" );
        var cacheTracer = mock( PageCacheTracer.class );
        var pageCursorTracer = mock( PageCursorTracer.class );
        when( cacheTracer.createPageCursorTracer( any() ) ).thenReturn( pageCursorTracer );
        var factory = mock( IdGeneratorFactory.class, RETURNS_MOCKS );
        var replicator = mock( Replicator.class );
        var stateMachineResult = mock( StateMachineResult.class );
        when( stateMachineResult.consume() ).thenReturn( 1 );
        var replicationResult = ReplicationResult.applied( stateMachineResult );
        when( replicator.replicate( any() ) ).thenReturn( replicationResult );

        var tokenHolder = new ReplicatedLabelTokenHolder( namedDatabaseId, registry, replicator, factory, storageEngineSupplier, cacheTracer, INSTANCE,
                                                          logEntryWriterFactory );
        tokenHolder.createToken( "foo", false );

        verify( cacheTracer ).createPageCursorTracer( any() );
        verify( pageCursorTracer ).close();
    }

    @Test
    void shouldReturnExistingTokenId() throws KernelException
    {
        // given
        TokenRegistry registry = new TokenRegistry( "Label" );
        ReplicatedTokenHolder tokenHolder = new ReplicatedLabelTokenHolder( namedDatabaseId, registry, null,
                null, storageEngineSupplier, PageCacheTracer.NULL, INSTANCE, logEntryWriterFactory );
        tokenHolder.setInitialTokens( asList( new NamedToken( "name1", 1 ), new NamedToken( "name2", 2 ) ) );

        // when
        Integer tokenId = tokenHolder.getOrCreateId( "name1" );

        // then
        assertThat( tokenId, equalTo( 1 ) );
    }

    @Test
    void shouldReplicateTokenRequestForNewToken() throws Exception
    {
        // given
        storageEngine = mockedStorageEngine();

        IdGeneratorFactory idGeneratorFactory = mock( IdGeneratorFactory.class );
        IdGenerator idGenerator = mock( IdGenerator.class );
        when( idGenerator.nextId( PageCursorTracer.NULL ) ).thenReturn( 1L );

        when( idGeneratorFactory.get( any( IdType.class ) ) ).thenReturn( idGenerator );

        TokenRegistry registry = new TokenRegistry( "Label" );
        int generatedTokenId = 1;
        ReplicatedTokenHolder tokenHolder = new ReplicatedLabelTokenHolder( namedDatabaseId, registry,
                content -> ReplicationResult.applied( StateMachineResult.of( generatedTokenId ) ), idGeneratorFactory, storageEngineSupplier,
                PageCacheTracer.NULL, INSTANCE, logEntryWriterFactory );

        // when
        Integer tokenId = tokenHolder.getOrCreateId( "name1" );

        // then
        assertThat( tokenId, equalTo( generatedTokenId ) );
    }

    private StorageEngine mockedStorageEngine() throws Exception
    {
        StorageEngine storageEngine = mock( StorageEngine.class );
        doAnswer( invocation ->
        {
            Collection<StorageCommand> target = invocation.getArgument( 0 );
            ReadableTransactionState txState = invocation.getArgument( 1 );
            txState.accept( new TxStateVisitor.Adapter()
            {
                @Override
                public void visitCreatedLabelToken( long id, String name, boolean internal )
                {
                    LabelTokenRecord before = new LabelTokenRecord( id );
                    LabelTokenRecord after = before.copy();
                    after.setInUse( true );
                    after.setInternal( internal );
                    target.add( new Command.LabelTokenCommand( before, after ) );
                }
            } );
            return null;
        } ).when( storageEngine ).createCommands( anyCollection(), any( ReadableTransactionState.class ),
                any( StorageReader.class ), any( CommandCreationContext.class ),
                any( ResourceLocker.class ), anyLong(), any( TxStateVisitor.Decorator.class ), any( PageCursorTracer.class ), any( MemoryTracker.class ) );

        StorageReader readLayer = mock( StorageReader.class );
        when( storageEngine.newReader() ).thenReturn( readLayer );
        return storageEngine;
    }
}
