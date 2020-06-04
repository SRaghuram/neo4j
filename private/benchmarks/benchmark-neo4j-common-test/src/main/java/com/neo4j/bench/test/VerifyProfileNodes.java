/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.test;

import com.neo4j.bench.client.queries.Query;
import org.junit.Assert;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import static org.neo4j.driver.AccessMode.READ;

public class VerifyProfileNodes implements Query<Void>
{
    private Path recordingsBasePath;

    public VerifyProfileNodes( Path recordingsBasePath )
    {
        this.recordingsBasePath = recordingsBasePath;
    }

    @Override
    public Void execute( Driver driver )
    {
        String query = "MATCH (p:Profiles) RETURN p{.*} as profiles";
        System.out.println( query );
        try ( Session session = driver.session( SessionConfig.builder().withDefaultAccessMode( READ ).build() ) )
        {
            List<Record> results = session.run( query ).list();
            for ( Record record : results )
            {
                Map<String,Object> profiles = record.get( "profiles" ).asMap();
                profiles.values().forEach( storePath -> Assert.assertTrue( Files.exists( recordingsBasePath.resolve( storePath.toString() ) ) ) );
            }
        }
        return null;
    }

    @Override
    public Optional<String> nonFatalError()
    {
        return Optional.empty();
    }
}
