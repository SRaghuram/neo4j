/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.lang.String.format;

public class ClientUtil
{
    public static String generateUniqueId()
    {
        return UUID.randomUUID().toString();
    }

    public static Map<String,String> pathToMap( Path path ) throws IOException
    {
        try ( FileInputStream is = new FileInputStream( path.toFile() ) )
        {
            Properties properties = new Properties();
            properties.load( is );
            return new HashMap<>( Maps.fromProperties( properties ) );
        }
    }

    public static String durationToString( long duration )
    {
        long m = TimeUnit.MILLISECONDS.toMinutes( duration );
        long s = TimeUnit.MILLISECONDS.toSeconds( duration ) - TimeUnit.MINUTES.toSeconds( m );
        return format( "%02d:%02d (m:s)", m, s );
    }

    public static <K, V> String prettyPrint( Map<K,V> map, String prefix )
    {
        List<Map.Entry<K,V>> mapEntries = sortedEntries( map );
        StringBuilder sb = new StringBuilder();
        for ( Map.Entry<K,V> entry : mapEntries )
        {
            String keyString = entry.getKey().toString();
            String valueString = (null == entry.getValue()) ? "null" : entry.getValue().toString();
            sb.append( prefix ).append( keyString ).append( " = " ).append( valueString ).append( "\n" );
        }
        return sb.toString();
    }

    private static <K, V> List<Map.Entry<K,V>> sortedEntries( Map<K,V> map )
    {
        List<Map.Entry<K,V>> sortedEntries = Lists.newArrayList( map.entrySet() );
        sortedEntries.sort( new EntriesComparator() );
        return sortedEntries;
    }

    private static class EntriesComparator implements Comparator<Map.Entry>
    {
        @Override
        public int compare( Map.Entry o1, Map.Entry o2 )
        {
            if ( o1.getKey() instanceof Comparable && o2.getKey() instanceof Comparable )
            {
                return ((Comparable) o1.getKey()).compareTo( o2.getKey() );
            }
            else
            {
                return o1.toString().compareTo( o2.toString() );
            }
        }
    }
}
