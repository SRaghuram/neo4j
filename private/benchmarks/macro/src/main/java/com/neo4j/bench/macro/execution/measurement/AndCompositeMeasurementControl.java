package com.neo4j.bench.macro.execution.measurement;

import static java.util.stream.Collectors.joining;

public class AndCompositeMeasurementControl extends CompositeMeasurementControl
{
    AndCompositeMeasurementControl( MeasurementControl... measurementControls )
    {
        super( measurementControls );
    }

    @Override
    public boolean isComplete()
    {
        return measurementControls.stream().allMatch( MeasurementControl::isComplete );
    }

    @Override
    public String description()
    {
        return "and( " + measurementControls.stream().map( MeasurementControl::description ).collect( joining( " , " ) ) + " )";
    }
}
