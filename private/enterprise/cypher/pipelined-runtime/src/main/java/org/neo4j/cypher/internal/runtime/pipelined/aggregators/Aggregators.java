/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators;

/**
 * This class is a necessary indirection for referencing the Aggregation case objects from generated code
 */
@SuppressWarnings( "unused" )
public class Aggregators
{
    public static final Aggregator COUNT_STAR = CountStarAggregator$.MODULE$;
    public static final Aggregator COUNT = CountAggregator$.MODULE$;
    public static final Aggregator COUNT_DISTINCT = CountDistinctAggregator$.MODULE$;
    public static final Aggregator SUM = SumAggregator$.MODULE$;
    public static final Aggregator SUM_DISTINCT = SumDistinctAggregator$.MODULE$;
    public static final Aggregator AVG = AvgAggregator$.MODULE$;
    public static final Aggregator AVG_DISTINCT = AvgDistinctAggregator$.MODULE$;
    public static final Aggregator MAX = MaxAggregator$.MODULE$;
    public static final Aggregator MIN = MinAggregator$.MODULE$;
    public static final Aggregator COLLECT = CollectAggregator$.MODULE$;
    public static final Aggregator COLLECT_ALL = CollectAllAggregator$.MODULE$;
    public static final Aggregator COLLECT_DISTINCT = CollectDistinctAggregator$.MODULE$;
    public static final Aggregator NON_EMPTY = NonEmptyAggregator$.MODULE$;
    public static final Aggregator IS_EMPTY = IsEmptyAggregator$.MODULE$;
    public static final Aggregator STDEV = StdevAggregator$.MODULE$;
    public static final Aggregator STDEV_DISTINCT = StdevDistinctAggregator$.MODULE$;
    public static final Aggregator STDEVP = StdevPAggregator$.MODULE$;
    public static final Aggregator STDEVP_DISTINCT = StdevPDistinctAggregator$.MODULE$;
}
