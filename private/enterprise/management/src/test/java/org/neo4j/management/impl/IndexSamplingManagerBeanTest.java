/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management.impl;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.TokenRead.NO_TOKEN;

public class IndexSamplingManagerBeanTest
{
    private static final String EXISTING_LABEL = "label";
    private static final String NON_EXISTING_LABEL = "bogusLabel";
    private static final int LABEL_ID = 42;
    private static final String EXISTING_PROPERTY = "prop";
    private static final String NON_EXISTING_PROPERTY = "bogusProp";
    private static final int PROPERTY_ID = 43;

    private NeoStoreDataSource dataSource;
    private IndexingService indexingService;

    @Before
    public void setup()
    {
        dataSource = mock( NeoStoreDataSource.class );
        StorageEngine storageEngine = mock( StorageEngine.class );
        StorageReader storageReader = mock( StorageReader.class );
        when( storageEngine.newReader() ).thenReturn( storageReader );
        indexingService = mock( IndexingService.class );
        TokenHolders tokenHolders = mockedTokenHolders();
        when( tokenHolders.labelTokens().getIdByName( EXISTING_LABEL ) ).thenReturn( LABEL_ID );
        when( tokenHolders.propertyKeyTokens().getIdByName( EXISTING_PROPERTY ) ).thenReturn( PROPERTY_ID );
        when( tokenHolders.propertyKeyTokens().getIdByName( NON_EXISTING_PROPERTY ) ).thenReturn( -1 );
        when( tokenHolders.labelTokens().getIdByName( NON_EXISTING_LABEL ) ).thenReturn( NO_TOKEN );
        Dependencies dependencies = mock( Dependencies.class );
        when( dependencies.resolveDependency( IndexingService.class ) ).thenReturn( indexingService );
        when( dependencies.resolveDependency( StorageEngine.class ) ).thenReturn( storageEngine );
        when( dependencies.resolveDependency( TokenHolders.class ) ).thenReturn( tokenHolders );
        when( dataSource.getDependencyResolver() ).thenReturn( dependencies );
    }

    @Test
    public void samplingTriggeredWhenIdsArePresent() throws IndexNotFoundKernelException
    {
        // Given
        IndexSamplingManagerBean.StoreAccess storeAccess = new IndexSamplingManagerBean.StoreAccess();
        storeAccess.registered( dataSource );

        // When
        storeAccess.triggerIndexSampling( EXISTING_LABEL, EXISTING_PROPERTY, false );

        // Then
        verify( indexingService, times( 1 ) ).triggerIndexSampling(
                SchemaDescriptorFactory.forLabel( LABEL_ID, PROPERTY_ID ) ,
                IndexSamplingMode.TRIGGER_REBUILD_UPDATED);
    }

    @Test
    public void forceSamplingTriggeredWhenIdsArePresent() throws IndexNotFoundKernelException
    {
        // Given
        IndexSamplingManagerBean.StoreAccess storeAccess = new IndexSamplingManagerBean.StoreAccess();
        storeAccess.registered( dataSource );

        // When
        storeAccess.triggerIndexSampling( EXISTING_LABEL, EXISTING_PROPERTY, true );

        // Then
        verify( indexingService, times( 1 ) ).triggerIndexSampling(
                SchemaDescriptorFactory.forLabel( LABEL_ID, PROPERTY_ID ) ,
                IndexSamplingMode.TRIGGER_REBUILD_ALL);
    }

    @Test( expected = IllegalArgumentException.class )
    public void exceptionThrownWhenMissingLabel()
    {
        // Given
        IndexSamplingManagerBean.StoreAccess storeAccess = new IndexSamplingManagerBean.StoreAccess();
        storeAccess.registered( dataSource );

        // When
        storeAccess.triggerIndexSampling( NON_EXISTING_LABEL, EXISTING_PROPERTY, false );
    }

    @Test( expected = IllegalArgumentException.class )
    public void exceptionThrownWhenMissingProperty()
    {
        // Given
        IndexSamplingManagerBean.StoreAccess storeAccess = new IndexSamplingManagerBean.StoreAccess();
        storeAccess.registered( dataSource );

        // When
        storeAccess.triggerIndexSampling( EXISTING_LABEL, NON_EXISTING_PROPERTY, false );
    }

    @Test( expected = IllegalArgumentException.class )
    public void exceptionThrownWhenNotRegistered()
    {
        // Given
        IndexSamplingManagerBean.StoreAccess storeAccess = new IndexSamplingManagerBean.StoreAccess();

        // When
        storeAccess.triggerIndexSampling( EXISTING_LABEL, EXISTING_PROPERTY, false );
    }

    private static TokenHolders mockedTokenHolders()
    {
        return new TokenHolders(
                mock( TokenHolder.class ),
                mock( TokenHolder.class ),
                mock( TokenHolder.class ) );
    }
}
