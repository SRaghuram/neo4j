/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.token;

import com.neo4j.causalclustering.core.replication.ReplicationResult;
import com.neo4j.causalclustering.core.state.StateMachineResult;
import org.junit.Test;

import java.util.Collection;
import java.util.function.Supplier;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.lock.ResourceLocker;
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
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReplicatedTokenHolderTest
{
    private StorageEngine storageEngine;
    private Supplier<StorageEngine> storageEngineSupplier = () -> storageEngine;
    private NamedDatabaseId namedDatabaseId = new TestDatabaseIdRepository().defaultDatabase();

    @Test
    public void shouldStoreInitialTokens()
    {
        // given
        TokenRegistry registry = new TokenRegistry( "Label" );
        ReplicatedTokenHolder tokenHolder = new ReplicatedLabelTokenHolder( namedDatabaseId, registry, null, null, storageEngineSupplier );

        // when
        tokenHolder.setInitialTokens( asList( new NamedToken( "name1", 1 ), new NamedToken( "name2", 2 ) ) );

        // then
        assertThat( tokenHolder.getAllTokens(), hasItems( new NamedToken( "name1", 1 ), new NamedToken( "name2", 2 ) ) );
    }

    @Test
    public void shouldReturnExistingTokenId() throws KernelException
    {
        // given
        TokenRegistry registry = new TokenRegistry( "Label" );
        ReplicatedTokenHolder tokenHolder = new ReplicatedLabelTokenHolder( namedDatabaseId, registry, null,
                null, storageEngineSupplier );
        tokenHolder.setInitialTokens( asList( new NamedToken( "name1", 1 ), new NamedToken( "name2", 2 ) ) );

        // when
        Integer tokenId = tokenHolder.getOrCreateId( "name1" );

        // then
        assertThat( tokenId, equalTo( 1 ) );
    }

    @Test
    public void shouldReplicateTokenRequestForNewToken() throws Exception
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
                content -> ReplicationResult.applied( StateMachineResult.of( generatedTokenId ) ), idGeneratorFactory, storageEngineSupplier );

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
                any( ResourceLocker.class ), anyLong(), any( TxStateVisitor.Decorator.class ) );

        StorageReader readLayer = mock( StorageReader.class );
        when( storageEngine.newReader() ).thenReturn( readLayer );
        return storageEngine;
    }
}
