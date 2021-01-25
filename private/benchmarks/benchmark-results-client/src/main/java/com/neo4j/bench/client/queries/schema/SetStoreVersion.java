/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries.schema;

import com.neo4j.bench.client.queries.Query;

import java.util.Optional;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import static java.util.Collections.singletonMap;
import static org.neo4j.driver.AccessMode.WRITE;

public class SetStoreVersion implements Query<Void>
{
    private final long version;

    public SetStoreVersion( long version )
    {
        this.version = version;
    }

    @Override
    public Void execute( Driver driver )
    {
        try ( Session session = driver.session( SessionConfig.builder().withDefaultAccessMode( WRITE ).build() ) )
        {
            session.run( "MERGE (ss:StoreSchema) SET ss.version=$version", singletonMap( "version", version ) );
        }
        return null;
    }

    @Override
    public Optional<String> nonFatalError()
    {
        return Optional.empty();
    }
}
