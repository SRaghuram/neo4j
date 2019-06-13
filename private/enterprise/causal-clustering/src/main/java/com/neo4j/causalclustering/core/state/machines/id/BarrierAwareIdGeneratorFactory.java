/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.id;

import java.io.File;
import java.nio.file.OpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongSupplier;

import com.neo4j.causalclustering.core.state.machines.barrier.BarrierState;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.DatabaseId;

public class BarrierAwareIdGeneratorFactory implements IdGeneratorFactory
{
    private final IdGeneratorFactory delegate;
    private final DatabaseId databaseId;
    private final BarrierState barrierState;

    private final Map<Pair<DatabaseId,IdType>,BarrierAwareIdGenerator> generators = new HashMap<>();

    public BarrierAwareIdGeneratorFactory( IdGeneratorFactory delegate, DatabaseId databaseId, BarrierState barrierState )
    {
        this.delegate = delegate;
        this.databaseId = databaseId;
        this.barrierState = barrierState;
    }

    @Override
    public IdGenerator open( PageCache pageCache, File fileName, IdType idType, LongSupplier highIdScanner, long maxId,
                             OpenOption... openOptions )
    {
        return generators.computeIfAbsent( Pair.of( databaseId, idType ), theIdType ->
                new BarrierAwareIdGenerator( delegate.open( pageCache, fileName, idType, highIdScanner, maxId, openOptions ), barrierState ) );
    }

    @Override
    public IdGenerator get( IdType idType )
    {
        return delegate.get( idType );
    }

    @Override
    public IdGenerator create( PageCache pageCache, File fileName, IdType idType, long highId, boolean throwIfFileExists,
                               long maxId, OpenOption... openOptions )
    {
        return generators.computeIfAbsent( Pair.of( databaseId, idType ), theIdType ->
                new BarrierAwareIdGenerator( delegate.create( pageCache, fileName, idType, highId, throwIfFileExists,
                        maxId, openOptions ), barrierState ) );
    }
}
