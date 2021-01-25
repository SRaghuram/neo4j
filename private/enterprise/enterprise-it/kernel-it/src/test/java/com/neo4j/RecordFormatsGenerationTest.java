/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.kernel.impl.store.format.highlimit.v300.HighLimitV3_0_0;
import com.neo4j.kernel.impl.store.format.highlimit.v306.HighLimitV3_0_6;
import com.neo4j.kernel.impl.store.format.highlimit.v310.HighLimitV3_1_0;
import com.neo4j.kernel.impl.store.format.highlimit.v320.HighLimitV3_2_0;
import com.neo4j.kernel.impl.store.format.highlimit.v340.HighLimitV3_4_0;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.StoreVersion;
import org.neo4j.kernel.impl.store.format.aligned.PageAlignedV4_1;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.kernel.impl.store.format.standard.StandardV4_0;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.impl.store.format.RecordFormats.NO_GENERATION;

class RecordFormatsGenerationTest
{
    @Test
    void correctGenerations()
    {
        List<RecordFormats> recordFormats = Arrays.asList(
                StandardV3_4.RECORD_FORMATS,
                StandardV4_0.RECORD_FORMATS,
                HighLimitV3_0_0.RECORD_FORMATS,
                HighLimitV3_0_6.RECORD_FORMATS,
                HighLimitV3_1_0.RECORD_FORMATS,
                HighLimitV3_2_0.RECORD_FORMATS,
                HighLimitV3_4_0.RECORD_FORMATS,
                HighLimit.RECORD_FORMATS,
                PageAlignedV4_1.RECORD_FORMATS
        );

        // Verify that our list above is complete.
        Set<RecordFormats> allActualFormats = StreamSupport.stream( RecordFormatSelector.allFormats().spliterator(), false )
                .filter( format -> format.generation() != NO_GENERATION )
                .collect( Collectors.toSet() );
        Set<RecordFormats> allExpectedFormats = new HashSet<>( recordFormats );
        assertEquals( allExpectedFormats, allActualFormats );

        Map<FormatFamily,List<Integer>> generationsForFamilies = recordFormats
                .stream()
                .filter( format -> format.generation() != NO_GENERATION )
                .collect( groupingBy( RecordFormats::getFormatFamily,
                        mapping( RecordFormats::generation, toList() ) ) );
        assertEquals( 3, generationsForFamilies.size() );
        for ( Map.Entry<FormatFamily,List<Integer>> familyListGeneration : generationsForFamilies.entrySet() )
        {
            assertEquals( familyListGeneration.getValue(), distinct( familyListGeneration.getValue() ),
                    "Generation inside format family should be unique." );
        }
    }

    @Test
    void uniqueGenerations()
    {
        Map<FormatFamily,List<Integer>> familyGenerations = allFamilyGenerations();
        for ( Map.Entry<FormatFamily,List<Integer>> familyEntry : familyGenerations.entrySet() )
        {
            assertEquals( familyEntry.getValue(), distinct( familyEntry.getValue() ), "Generation inside format family should be unique." );
        }
    }

    private static Map<FormatFamily, List<Integer>> allFamilyGenerations()
    {
        return Arrays.stream( StoreVersion.values() )
                .map( StoreVersion::versionString )
                .map( RecordFormatSelector::selectForVersion )
                .collect( groupingBy( RecordFormats::getFormatFamily,  mapping( RecordFormats::generation, toList() ) ) );
    }

    private static List<Integer> distinct( List<Integer> integers )
    {
        return integers.stream()
                .distinct()
                .collect( toList() );
    }
}
