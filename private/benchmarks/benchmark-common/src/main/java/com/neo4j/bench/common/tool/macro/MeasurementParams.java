/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.tool.macro;

import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class MeasurementParams
{
    public static final String CMD_WARMUP = "--warmup-count";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WARMUP},
            title = "Warmup execution count" )
    @Required
    private int warmupCount;

    public static final String CMD_MEASUREMENT = "--measurement-count";
    @Option( type = OptionType.COMMAND,
            name = {CMD_MEASUREMENT},
            title = "Measurement execution count" )
    @Required
    private int measurementCount;

    public static final String CMD_MIN_MEASUREMENT_DURATION = "--min-measurement-duration";
    @Option( type = OptionType.COMMAND,
            name = {CMD_MIN_MEASUREMENT_DURATION},
            title = "Min measurement execution duration, in seconds" )
    private int minMeasurementSeconds = 30; // 30 seconds

    public static final String CMD_MAX_MEASUREMENT_DURATION = "--max-measurement-duration";
    @Option( type = OptionType.COMMAND,
            name = {CMD_MAX_MEASUREMENT_DURATION},
            title = "Max measurement execution duration, in seconds" )
    private int maxMeasurementSeconds = 10 * 60; // 10 minutes

    public MeasurementParams()
    {
        // default constructor for command line arguments
    }

    public MeasurementParams( int warmupCount, int measurementCount, Duration minMeasurementDuration, Duration maxMeasurementDuration )
    {
        this.warmupCount = warmupCount;
        this.measurementCount = measurementCount;
        this.minMeasurementSeconds = (int) minMeasurementDuration.getSeconds();
        this.maxMeasurementSeconds = (int) maxMeasurementDuration.getSeconds();
    }

    public int warmupCount()
    {
        return warmupCount;
    }

    public int measurementCount()
    {
        return measurementCount;
    }

    public int minMeasurementSeconds()
    {
        return minMeasurementSeconds;
    }

    public int maxMeasurementSeconds()
    {
        return maxMeasurementSeconds;
    }

    public List<String> asArgs()
    {
        return asMap().entrySet()
                      .stream()
                      .flatMap( entry -> Stream.of( entry.getKey(), entry.getValue() ) )
                      .collect( toList() );
    }

    public Map<String,String> asMap()
    {
        Map<String,String> map = new HashMap<>();
        map.put( CMD_WARMUP, Integer.toString( warmupCount ) );
        map.put( CMD_MEASUREMENT, Integer.toString( measurementCount ) );
        map.put( CMD_MIN_MEASUREMENT_DURATION, Long.toString( minMeasurementSeconds ) );
        map.put( CMD_MAX_MEASUREMENT_DURATION, Long.toString( maxMeasurementSeconds ) );
        return map;
    }

    @Override
    public boolean equals( Object that )
    {
        return EqualsBuilder.reflectionEquals( this, that );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString( this, ToStringStyle.SHORT_PREFIX_STYLE );
    }
}
