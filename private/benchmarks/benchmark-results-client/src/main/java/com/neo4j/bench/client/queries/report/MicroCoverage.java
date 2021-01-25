/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries.report;

import com.neo4j.bench.client.queries.Query;
import com.neo4j.bench.client.queries.report.MicroCoverageResult.Change;
import com.neo4j.bench.model.model.Repository;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.driver.Driver;

import static java.lang.String.format;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

public class MicroCoverage implements Query<List<MicroCoverageResult>>, CsvHeader
{
    private final String oldNeo4jVersion;
    private final String newNeo4jVersion;
    private final double minDifference;

    public MicroCoverage( String oldNeo4jVersion, String newNeo4jVersion, double minDifference )
    {
        Repository.MICRO_BENCH.assertValidVersion( oldNeo4jVersion );
        Repository.MICRO_BENCH.assertValidVersion( newNeo4jVersion );
        this.oldNeo4jVersion = oldNeo4jVersion;
        this.newNeo4jVersion = newNeo4jVersion;
        this.minDifference = minDifference;
    }

    @Override
    public List<MicroCoverageResult> execute( Driver driver )
    {
        MicroComparison microComparison = new MicroComparison( oldNeo4jVersion, newNeo4jVersion, 1D );
        List<MicroComparisonResult> microComparisonResults = microComparison.execute( driver );
        Map<String,List<MicroComparisonResult>> benchmarkGroups = microComparisonResults.stream()
                                                                                        .collect( groupingBy( row -> row.group() + row.benchSimple() ) );
        return benchmarkGroups.values().stream()
                              .map( rows ->
                                    {
                                        String group = rows.get( 0 ).group();
                                        String bench = rows.get( 0 ).benchSimple();
                                        int testCount = rows.size();
                                        double min = rows.stream()
                                                         .mapToDouble( MicroComparisonResult::improvement )
                                                         // any change below the configured threshold is considered unchanged
                                                         .map( improvement -> (improvement > -minDifference) ? 0 : improvement )
                                                         .min()
                                                         .orElseThrow( () -> new RuntimeException( "This is surprising, there should be at least one row!" ) );
                                        double max = rows.stream()
                                                         .mapToDouble( MicroComparisonResult::improvement )
                                                         // any change below the configured threshold is considered unchanged
                                                         .map( improvement -> (improvement < minDifference) ? 0 : improvement )
                                                         .max()
                                                         .orElseThrow( () -> new RuntimeException( "This is surprising, there should be at least one row!" ) );
                                        Change change = computeChange( min, max );
                                        return new MicroCoverageResult( group, bench, testCount, change );
                                    } )
                              .sorted( new ResultComparator() )
                              .collect( toList() );
    }

    @Override
    public Optional<String> nonFatalError()
    {
        return Optional.empty();
    }

    private static Change computeChange( double minImprovement, double maxImprovement )
    {
        if ( minImprovement >= 0 && maxImprovement > 0 )
        {
            return Change.BETTER;
        }
        else if ( minImprovement == 0 && maxImprovement == 0 )
        {
            return Change.NO_CHANGE;
        }
        else if ( minImprovement < 0 && maxImprovement > 0 )
        {
            return Change.MIXED;
        }
        else if ( minImprovement < 0 && maxImprovement <= 0 )
        {
            return Change.WORSE;
        }
        else
        {
            throw new RuntimeException( format( "This is surprising, we should never fall into this state!\n" +
                                                "Min = %s\n" +
                                                "Max = %s", minImprovement, maxImprovement ) );
        }
    }

    @Override
    public String header()
    {
        return MicroCoverageResult.HEADER;
    }

    private static class ResultComparator implements Comparator<MicroCoverageResult>
    {
        @Override
        public int compare( MicroCoverageResult o1, MicroCoverageResult o2 )
        {
            // alphabetic ascending
            int groupCompare = o1.group().compareTo( o2.group() );
            if ( groupCompare != 0 )
            {
                return groupCompare;
            }
            else
            {
                // BETTER before WORSE
                int changeCompare = o1.change().compareTo( o2.change() );
                if ( changeCompare != 0 )
                {
                    return changeCompare;
                }
                else
                {
                    // HIGH before LOW
                    int testCountCompare = Integer.compare( o2.testCount(), o1.testCount() );
                    if ( testCountCompare != 0 )
                    {
                        return testCountCompare;
                    }
                    else
                    {
                        // alphabetic ascending
                        return o1.bench().compareTo( o2.bench() );
                    }
                }
            }
        }
    }
}
