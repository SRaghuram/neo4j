/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management.impl;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.management.impl.IndexSamplingManagerBean.StoreAccess;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.schema.SchemaDescriptorFactory;
import org.neo4j.token.api.TokenHolder;

import static org.mockito.Mockito.mock;
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

    private Database database;
    private IndexingService indexingService;

    @Before
    public void setup()
    {
        Dependencies dependencies = mock( Dependencies.class );
        DatabaseContext databaseContext = mock( DatabaseContext.class );
        database = mock( Database.class );
        when( databaseContext.getDependencies() ).thenReturn( dependencies );
        when( dependencies.resolveDependency( Database.class ) ).thenReturn( database );
        StorageEngine storageEngine = mock( StorageEngine.class );
        StorageReader storageReader = mock( StorageReader.class );
        when( storageEngine.newReader() ).thenReturn( storageReader );
        indexingService = mock( IndexingService.class );
        TokenHolders tokenHolders = mockedTokenHolders();
        when( tokenHolders.labelTokens().getIdByName( EXISTING_LABEL ) ).thenReturn( LABEL_ID );
        when( tokenHolders.propertyKeyTokens().getIdByName( EXISTING_PROPERTY ) ).thenReturn( PROPERTY_ID );
        when( tokenHolders.propertyKeyTokens().getIdByName( NON_EXISTING_PROPERTY ) ).thenReturn( -1 );
        when( tokenHolders.labelTokens().getIdByName( NON_EXISTING_LABEL ) ).thenReturn( NO_TOKEN );
        when( dependencies.resolveDependency( IndexingService.class ) ).thenReturn( indexingService );
        when( dependencies.resolveDependency( StorageEngine.class ) ).thenReturn( storageEngine );
        when( dependencies.resolveDependency( TokenHolders.class ) ).thenReturn( tokenHolders );
        when( database.getDependencyResolver() ).thenReturn( dependencies );
    }

    @Test
    public void samplingTriggeredWhenIdsArePresent() throws IndexNotFoundKernelException
    {
        // Given
        StoreAccess storeAccess = new StoreAccess( database );

        // When
        storeAccess.triggerIndexSampling( EXISTING_LABEL, EXISTING_PROPERTY, false );

        // Then
        verify( indexingService ).triggerIndexSampling(
                SchemaDescriptorFactory.forLabel( LABEL_ID, PROPERTY_ID ) ,
                IndexSamplingMode.TRIGGER_REBUILD_UPDATED);
    }

    @Test
    public void forceSamplingTriggeredWhenIdsArePresent() throws IndexNotFoundKernelException
    {
        // Given
        StoreAccess storeAccess = new StoreAccess( database );

        // When
        storeAccess.triggerIndexSampling( EXISTING_LABEL, EXISTING_PROPERTY, true );

        // Then
        verify( indexingService ).triggerIndexSampling(
                SchemaDescriptorFactory.forLabel( LABEL_ID, PROPERTY_ID ) ,
                IndexSamplingMode.TRIGGER_REBUILD_ALL);
    }

    @Test( expected = IllegalArgumentException.class )
    public void exceptionThrownWhenMissingLabel()
    {
        // Given
        StoreAccess storeAccess = new StoreAccess( database );

        // When
        storeAccess.triggerIndexSampling( NON_EXISTING_LABEL, EXISTING_PROPERTY, false );
    }

    @Test( expected = IllegalArgumentException.class )
    public void exceptionThrownWhenMissingProperty()
    {
        // Given
        StoreAccess storeAccess = new StoreAccess( database );

        // When
        storeAccess.triggerIndexSampling( EXISTING_LABEL, NON_EXISTING_PROPERTY, false );
    }

    private static TokenHolders mockedTokenHolders()
    {
        return new TokenHolders(
                mock( TokenHolder.class ),
                mock( TokenHolder.class ),
                mock( TokenHolder.class ) );
    }
}
