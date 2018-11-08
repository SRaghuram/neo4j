/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.id;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongSupplier;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfiguration;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.logging.LogProvider;

public class ReplicatedIdGeneratorFactory implements IdGeneratorFactory
{
    private final Map<Pair<String,IdType>,ReplicatedIdGenerator> generators = new HashMap<>();
    private final FileSystemAbstraction fs;
    private final Function<String,ReplicatedIdRangeAcquirer> idRangeAcquirer;
    private final LogProvider logProvider;
    private IdTypeConfigurationProvider idTypeConfigurationProvider;
    private final String databaseName;

    public ReplicatedIdGeneratorFactory( FileSystemAbstraction fs, Function<String,ReplicatedIdRangeAcquirer> idRangeAcquirer,
            LogProvider logProvider, IdTypeConfigurationProvider idTypeConfigurationProvider, String databaseName )
    {
        this.fs = fs;
        this.idRangeAcquirer = idRangeAcquirer;
        this.logProvider = logProvider;
        this.idTypeConfigurationProvider = idTypeConfigurationProvider;
        this.databaseName = databaseName;
    }

    @Override
    public IdGenerator open( File file, IdType idType, LongSupplier highId, long maxId )
    {
        IdTypeConfiguration idTypeConfiguration = idTypeConfigurationProvider.getIdTypeConfiguration( idType );
        return openGenerator( file, idTypeConfiguration.getGrabSize(), idType, highId, maxId, idTypeConfiguration.allowAggressiveReuse() );
    }

    @Override
    public IdGenerator open( File fileName, int grabSize, IdType idType, LongSupplier highId, long maxId )
    {
        IdTypeConfiguration idTypeConfiguration = idTypeConfigurationProvider.getIdTypeConfiguration( idType );
        return openGenerator( fileName, grabSize, idType, highId, maxId, idTypeConfiguration.allowAggressiveReuse() );
    }

    // TODO: Investigate why maxId is not used.
    private IdGenerator openGenerator( File file, int grabSize, IdType idType, LongSupplier highId, long maxId, boolean aggressiveReuse )
    {
        ReplicatedIdGenerator other = generators.get( Pair.of( databaseName, idType ) );
        if ( other != null )
        {
            other.close();
        }
        ReplicatedIdGenerator replicatedIdGenerator = new ReplicatedIdGenerator( fs, file, idType, highId,
                idRangeAcquirer.apply( databaseName ), logProvider, grabSize, aggressiveReuse );

        generators.put( Pair.of( databaseName, idType ), replicatedIdGenerator);
        return replicatedIdGenerator;
    }

    @Override
    public IdGenerator get( IdType idType )
    {
        return get( databaseName, idType );
    }

    public IdGenerator get( String databaseName, IdType idType )
    {
        return generators.get( Pair.of( databaseName, idType ) );
    }

    @Override
    public void create( File fileName, long highId, boolean throwIfFileExists )
    {
        ReplicatedIdGenerator.createGenerator( fs, fileName, highId, throwIfFileExists );
    }
}
