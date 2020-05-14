/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries.report;

import com.neo4j.bench.client.queries.Query;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.common.util.Units;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.Repository;
import com.neo4j.bench.model.util.UnitConverter;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.SessionConfig;

import static java.util.stream.Collectors.toList;
import static org.neo4j.driver.AccessMode.READ;

public class MacroComparison implements Query<List<MacroComparisonResult>>, CsvHeader
{
    private static final String QUERY = Resources.fileToString( "/queries/report/macro_comparison.cypher" );

    private final String oldNeo4jVersion;
    private final String newNeo4jVersion;
    private final double minDifference;

    public MacroComparison( String oldNeo4jVersion, String newNeo4jVersion, double minDifference )
    {
        Repository.MICRO_BENCH.assertValidVersion( oldNeo4jVersion );
        Repository.MICRO_BENCH.assertValidVersion( newNeo4jVersion );
        this.oldNeo4jVersion = oldNeo4jVersion;
        this.newNeo4jVersion = newNeo4jVersion;
        this.minDifference = minDifference;
    }

    @Override
    public List<MacroComparisonResult> execute( Driver driver )
    {
        try ( Session session = driver.session( SessionConfig.builder().withDefaultAccessMode( READ ).build() ) )
        {
            Map<String,Object> params = new HashMap<>();
            params.put( "old_version", oldNeo4jVersion );
            params.put( "new_version", newNeo4jVersion );
            Result result = session.run( QUERY, params );
            return result.list().stream()
                         .map( row ->
                               {
                                   String group = row.get( "group" ).asString();
                                   String bench = row.get( "bench" ).asString();
                                   String description = row.get( "description" ).asString();
                                   Benchmark.Mode mode = Benchmark.Mode.valueOf( row.get( "mode" ).asString() );
                                   double oldResult = row.get( "old" ).asDouble();
                                   double newResult = row.get( "new" ).asDouble();
                                   TimeUnit oldUnit = UnitConverter.toTimeUnit( row.get( "old_unit" ).asString() );
                                   TimeUnit newUnit = UnitConverter.toTimeUnit( row.get( "new_unit" ).asString() );
                                   TimeUnit saneOldUnit = Units.findSaneUnit( oldResult, oldUnit, mode, 1, 1000 );
                                   TimeUnit saneNewUnit = Units.findSaneUnit( newResult, newUnit, mode, 1, 1000 );
                                   TimeUnit commonUnit = Units.maxValueUnit( saneOldUnit, saneNewUnit, mode );
                                   double convertedOldResult = Units.convertValueTo( oldResult, oldUnit, commonUnit, mode );
                                   double convertedNewResult = Units.convertValueTo( newResult, newUnit, commonUnit, mode );
                                   double improvement = Units.improvement( convertedOldResult, convertedNewResult, mode );
                                   return new MacroComparisonResult( group,
                                                                     bench,
                                                                     description,
                                                                     convertedOldResult,
                                                                     convertedNewResult,
                                                                     Units.toAbbreviation( commonUnit, mode ),
                                                                     improvement );
                               } )
                         .filter( row -> Math.abs( row.improvement() ) >= minDifference )
                         .sorted( new ResultComparator() )
                         .collect( toList() );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error retrieving benchmark results", e );
        }
    }

    @Override
    public Optional<String> nonFatalError()
    {
        return Optional.empty();
    }

    @Override
    public String header()
    {
        return MacroComparisonResult.HEADER;
    }

    private static class ResultComparator implements Comparator<MacroComparisonResult>
    {
        @Override
        public int compare( MacroComparisonResult o1, MacroComparisonResult o2 )
        {
            // alphabetic ascending
            int groupCompare = o1.group().compareTo( o2.group() );
            if ( groupCompare != 0 )
            {
                return groupCompare;
            }
            else
            {
                // HIGH before LOW
                return Double.compare( o2.improvement(), o1.improvement() );
            }
        }
    }
}
