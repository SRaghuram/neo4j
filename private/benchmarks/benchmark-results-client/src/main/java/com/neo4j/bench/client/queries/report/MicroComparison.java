/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries.report;

import com.neo4j.bench.client.queries.Query;
import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.Repository;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.common.util.Units;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import static java.util.stream.Collectors.toList;
import static org.neo4j.driver.v1.AccessMode.READ;

public class MicroComparison implements Query<List<MicroComparisonResult>>
{
    private static final String QUERY = Resources.fileToString( "/queries/report/micro_comparison.cypher" );

    private final String oldNeo4jVersion;
    private final String newNeo4jVersion;
    private final double minDifference;

    public MicroComparison( String oldNeo4jVersion, String newNeo4jVersion )
    {
        this( oldNeo4jVersion, newNeo4jVersion, 1 );
    }

    public MicroComparison( String oldNeo4jVersion, String newNeo4jVersion, double minDifference )
    {
        Repository.MICRO_BENCH.assertValidVersion( oldNeo4jVersion );
        this.oldNeo4jVersion = oldNeo4jVersion;
        this.newNeo4jVersion = newNeo4jVersion;
        this.minDifference = minDifference;
    }

    @Override
    public List<MicroComparisonResult> execute( Driver driver )
    {
        try ( Session session = driver.session( READ ) )
        {
            Map<String,Object> params = new HashMap<>();
            params.put( "old_version", oldNeo4jVersion );
            params.put( "new_version", newNeo4jVersion );
            StatementResult result = session.run( QUERY, params );
            return result.list().stream()
                         .map( row ->
                               {
                                   String group = row.get( "group" ).asString();
                                   String bench = row.get( "bench" ).asString();
                                   Benchmark.Mode mode = Benchmark.Mode.valueOf( row.get( "mode" ).asString() );
                                   double oldResult = row.get( "old" ).asDouble();
                                   double newResult = row.get( "new" ).asDouble();
                                   TimeUnit oldUnit = Units.toTimeUnit( row.get( "old_unit" ).asString() );
                                   TimeUnit newUnit = Units.toTimeUnit( row.get( "new_unit" ).asString() );
                                   TimeUnit saneOldUnit = Units.findSaneUnit( oldResult, oldUnit, mode, 1, 1000 );
                                   TimeUnit saneNewUnit = Units.findSaneUnit( newResult, newUnit, mode, 1, 1000 );
                                   TimeUnit commonUnit = Units.maxValueUnit( saneOldUnit, saneNewUnit, mode );
                                   double convertedOldResult = Units.convertValueTo( oldResult, oldUnit, commonUnit, mode );
                                   double convertedNewResult = Units.convertValueTo( newResult, newUnit, commonUnit, mode );
                                   double improvement = Units.improvement( convertedOldResult, convertedNewResult, mode );
                                   return new MicroComparisonResult( group,
                                                                     bench,
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
            throw new RuntimeException( "Error retrieving benchmark groups", e );
        }
    }

    @Override
    public Optional<String> nonFatalError()
    {
        return Optional.empty();
    }

    private static class ResultComparator implements Comparator<MicroComparisonResult>
    {

        @Override
        public int compare( MicroComparisonResult o1, MicroComparisonResult o2 )
        {
            int groupCompare = o1.group().compareTo( o2.group() );
            if ( groupCompare != 0 )
            {
                return groupCompare;
            }
            else
            {
                return Double.compare( o2.improvement(), o1.improvement() );
            }
        }

        @Override
        public boolean equals( Object obj )
        {
            return false;
        }
    }
}
