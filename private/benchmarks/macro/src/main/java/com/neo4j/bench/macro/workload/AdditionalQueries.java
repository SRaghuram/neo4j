/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.workload;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toList;

class AdditionalQueries
{
    private final String workloadName;
    private final List<Query> queries;

    private AdditionalQueries( String workloadName, List<Query> queries )
    {
        this.workloadName = workloadName;
        this.queries = queries;
    }

    private String workloadName()
    {
        return workloadName;
    }

    private List<Query> queries()
    {
        return queries;
    }

    private static final List<Query> GENERATED_QUERIES =
            Arrays.asList(
                    newQuery(
                            "generated_queries",
                            "project varying property",
                            ChangingQueryString.atDefaults(
                                    new IncrementingString( "MATCH (p:Person)\n" +
                                                            "RETURN p.prop%s" ) ) ),
                    newQuery(
                            "generated_queries",
                            "project varying relationship type - short pattern",
                            ChangingQueryString.atDefaults(
                                    new IncrementingString( "MATCH (p1:Person)-[:REL%s]-(p2:Person)\n" +
                                                            "RETURN p1.name=p2.name" ) ) ),
                    newQuery(
                            "generated_queries",
                            "project varying relationship type - long pattern",
                            ChangingQueryString.atDefaults(
                                    new IncrementingString( "MATCH (p1:Person)-[:R%s]-(:Person)-[:KNOWS]-(:Person)-[:KNOWS]-(:Person)-[:KNOWS]-(p2:Person)\n" +
                                                            "RETURN p1.name=p2.name" ) ) )
            );

    private static final List<AdditionalQueries> ADDITIONAL_WORKLOAD_QUERIES = Lists.newArrayList(
            new AdditionalQueries( "generated_queries", GENERATED_QUERIES )
    );

    static List<Query> queriesFor( String workloadName )
    {
        return ADDITIONAL_WORKLOAD_QUERIES.stream()
                                          .filter( queries -> queries.workloadName().equalsIgnoreCase( workloadName ) )
                                          .map( AdditionalQueries::queries )
                                          .flatMap( Collection::stream )
                                          .collect( toList() );
    }

    private static class IncrementingString implements Supplier<String>
    {
        private final String baseString;
        private int counter;

        private IncrementingString( String baseString )
        {
            this.baseString = baseString;
        }

        @Override
        public String get()
        {
            return String.format( baseString, Integer.toString( counter++ ) );
        }
    }

    private static Query newQuery( String group, String name, QueryString queryString )
    {
        return new Query( group, name, "", queryString, queryString, false, true, false, Parameters.empty() );
    }
}
