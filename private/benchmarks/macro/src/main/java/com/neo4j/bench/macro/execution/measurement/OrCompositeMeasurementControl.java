/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.measurement;

import static java.util.stream.Collectors.joining;

public class OrCompositeMeasurementControl extends CompositeMeasurementControl
{
    OrCompositeMeasurementControl( MeasurementControl... measurementControls )
    {
        super( measurementControls );
    }

    @Override
    public boolean isComplete()
    {
        return measurementControls.stream().anyMatch( MeasurementControl::isComplete );
    }

    @Override
    public String description()
    {
        return "or( " + measurementControls.stream().map( MeasurementControl::description ).collect( joining( " , " ) ) + " )";
    }
}
