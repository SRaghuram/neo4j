/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.util;

import com.google.common.collect.Lists;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class MapPrinter
{
    public static <K, V> String prettyPrint( Map<K,V> map )
    {
        return prettyPrint( map, "" );
    }

    private static <K, V> String prettyPrint( Map<K,V> map, String prefix )
    {
        List<Map.Entry<K,V>> mapEntries = sortedEntrySet( map );
        StringBuilder sb = new StringBuilder();
        for ( Map.Entry<K,V> entry : mapEntries )
        {
            String keyString = (null == entry.getKey()) ? "null" : entry.getKey().toString();
            String valueString = (null == entry.getValue()) ? "null" : entry.getValue().toString();
            sb.append( prefix ).append( keyString ).append( " = " ).append( valueString ).append( "\n" );
        }
        return sb.toString();
    }

    private static <K, V> List<Map.Entry<K,V>> sortedEntrySet( Map<K,V> map )
    {
        List<Map.Entry<K,V>> sortedEntries = Lists.newArrayList( map.entrySet() );
        sortedEntries.sort( new EntriesComparator<>() );
        return sortedEntries;
    }

    private static class EntriesComparator<K> implements Comparator<Map.Entry<K,?>>
    {
        @Override
        public int compare( Map.Entry<K,?> o1, Map.Entry<K,?> o2 )
        {
            if ( o1.getKey() instanceof Comparable )
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
