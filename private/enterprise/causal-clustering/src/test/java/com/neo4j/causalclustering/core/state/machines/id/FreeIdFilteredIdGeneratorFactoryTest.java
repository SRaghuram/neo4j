/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.id;

import org.junit.Test;

import java.io.File;
import java.util.function.LongSupplier;

import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.pagecache.PageCache;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class FreeIdFilteredIdGeneratorFactoryTest
{
    private final IdGeneratorFactory idGeneratorFactory = mock( IdGeneratorFactory.class );
    private final File file = mock( File.class );

    @Test
    public void openFilteredGenerator()
    {
        FreeIdFilteredIdGeneratorFactory filteredGenerator = createFilteredFactory();
        IdType idType = IdType.NODE;
        long highId = 1L;
        long maxId = 10L;
        LongSupplier highIdSupplier = () -> highId;
        PageCache pageCache = mock( PageCache.class );
        IdGenerator idGenerator = filteredGenerator.open( pageCache, file, idType, highIdSupplier, maxId );

        verify( idGeneratorFactory ).open( eq( pageCache ), eq( file ), eq( idType ), any( LongSupplier.class ), eq( maxId ) );
        assertThat( idGenerator, instanceOf( FreeIdFilteredIdGenerator.class ) );
    }

    @Test
    public void openFilteredGeneratorWithGrabSize()
    {
        FreeIdFilteredIdGeneratorFactory filteredGenerator = createFilteredFactory();
        IdType idType = IdType.NODE;
        long highId = 1L;
        long maxId = 10L;
        LongSupplier highIdSupplier = () -> highId;
        PageCache pageCache = mock( PageCache.class );
        IdGenerator idGenerator = filteredGenerator.open( pageCache, file, idType, highIdSupplier, maxId );

        verify( idGeneratorFactory ).open( pageCache, file, idType, highIdSupplier, maxId );
        assertThat( idGenerator, instanceOf( FreeIdFilteredIdGenerator.class ) );
    }

    private FreeIdFilteredIdGeneratorFactory createFilteredFactory()
    {
        return new FreeIdFilteredIdGeneratorFactory( idGeneratorFactory, () -> true );
    }
}
