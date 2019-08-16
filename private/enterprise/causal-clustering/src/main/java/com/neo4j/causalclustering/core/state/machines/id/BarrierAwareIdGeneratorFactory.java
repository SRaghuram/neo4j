/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.id;

import com.neo4j.causalclustering.core.state.machines.barrier.BarrierState;

import java.io.File;
import java.nio.file.OpenOption;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.LongSupplier;

import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.pagecache.PageCache;

public class BarrierAwareIdGeneratorFactory implements IdGeneratorFactory
{
    private final IdGeneratorFactory delegate;
    private final BarrierState barrierState;

    private final Map<IdType,BarrierAwareIdGenerator> generators = new EnumMap<>( IdType.class );

    public BarrierAwareIdGeneratorFactory( IdGeneratorFactory delegate, BarrierState barrierState )
    {
        this.delegate = delegate;
        this.barrierState = barrierState;
    }

    @Override
    public IdGenerator open( PageCache pageCache, File fileName, IdType idType, LongSupplier highIdScanner, long maxId, OpenOption... openOptions )
    {
        BarrierAwareIdGenerator idGenerator = new BarrierAwareIdGenerator(
                delegate.open( pageCache, fileName, idType, highIdScanner, maxId, openOptions ), barrierState );
        generators.put( idType, idGenerator );
        return idGenerator;
    }

    @Override
    public IdGenerator get( IdType idType )
    {
        return generators.get( idType );
    }

    @Override
    public IdGenerator create( PageCache pageCache, File fileName, IdType idType, long highId, boolean throwIfFileExists, long maxId,
            OpenOption... openOptions )
    {
        BarrierAwareIdGenerator idGenerator = new BarrierAwareIdGenerator(
                delegate.create( pageCache, fileName, idType, highId, throwIfFileExists, maxId, openOptions ), barrierState );
        generators.put( idType, idGenerator );
        return idGenerator;
    }
}
