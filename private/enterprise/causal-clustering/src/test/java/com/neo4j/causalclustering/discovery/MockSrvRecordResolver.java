/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class MockSrvRecordResolver extends SrvRecordResolver
{
    private final Map<String,List<SrvRecord>> records;

    public MockSrvRecordResolver( Map<String,List<SrvRecord>> records )
    {
        this.records = records;
    }

    public void addRecords( String url, Collection<SrvRecord> records )
    {
        records.forEach( r -> addRecord( url, r ) );
    }

    public synchronized void addRecord( String url, SrvRecord record )
    {
        List<SrvRecord> srvRecords = records.getOrDefault( url, new ArrayList<>() );
        srvRecords.add( record );

        if ( !records.containsKey( url ) )
        {
            records.put( url, srvRecords );
        }
    }

    @Override
    public Stream<SrvRecord> resolveSrvRecord( String url )
    {
        return Optional.ofNullable( records.get( url ) ).map( List::stream ).orElse( Stream.empty() );
    }
}
