package com.neo4j.bench.procedures.detection;

import com.neo4j.bench.client.Units;
import com.neo4j.bench.client.model.Benchmark.Mode;

import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import static com.neo4j.bench.client.Units.toTimeUnit;

public class DateTimeFunction
{
    @UserFunction( name = "bench.convert" )
    public double convert( @Name( "value" ) double value, @Name( "fromUnit" ) String fromUnit, @Name( "toUnit" ) String toUnit, @Name( "mode" ) String mode )
    {
        double conversionFactor = Units.conversionFactor( toTimeUnit( fromUnit ), toTimeUnit( toUnit ), Mode.valueOf( mode ) );
        return value * conversionFactor;
    }
}
