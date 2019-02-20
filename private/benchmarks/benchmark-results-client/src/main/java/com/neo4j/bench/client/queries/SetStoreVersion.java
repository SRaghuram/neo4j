/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries;

import java.util.Optional;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;

import static java.util.Collections.singletonMap;

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
        try ( Session session = driver.session() )
        {
            session.run( "MERGE (ss:StoreSchema) SET ss.version={version}", singletonMap( "version", version ) );
        }
        return null;
    }

    @Override
    public Optional<String> nonFatalError()
    {
        return Optional.empty();
    }
}
