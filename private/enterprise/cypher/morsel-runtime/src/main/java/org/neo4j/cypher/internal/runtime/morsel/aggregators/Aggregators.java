/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.aggregators;

/**
 * This class is a necessary indirection for referencing the Aggregation case objects from generated code
 */
public class Aggregators
{
    public static final Aggregator COUNT_STAR = CountStarAggregator$.MODULE$;
    public static final Aggregator COUNT = CountAggregator$.MODULE$;
    public static final Aggregator SUM = SumAggregator$.MODULE$;
    public static final Aggregator AVG = AvgAggregator$.MODULE$;
    public static final Aggregator MAX = MaxAggregator$.MODULE$;
    public static final Aggregator MIN = MaxAggregator$.MODULE$;
    public static final Aggregator COLLECT = CollectAggregator$.MODULE$;
}
