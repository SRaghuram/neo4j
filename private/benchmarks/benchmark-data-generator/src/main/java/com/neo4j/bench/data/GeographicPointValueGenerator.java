/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.data;

import java.util.SplittableRandom;

import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;

import static org.neo4j.values.storable.Values.pointValue;

public class GeographicPointValueGenerator implements ValueGeneratorFun<Point>
{
    private final CoordinateReferenceSystem crs;
    private final double minY;
    private final double minX;

    private final long xStepCount;
    private final long totalStepCount;

    private final double xStep;
    private final double yStep;

    private double x;
    private double y;
    private long currentCount;

    GeographicPointValueGenerator( CoordinateReferenceSystem crs, PointValue wrappedMinPoint, double xStep, double yStep, long xStepCount, long totalStepCount )
    {
        this.crs = crs;
        this.minX = wrappedMinPoint.coordinate()[0];
        this.minY = wrappedMinPoint.coordinate()[1];
        this.x = minX;
        this.y = minY;
        this.xStepCount = xStepCount;
        this.totalStepCount = totalStepCount;
        this.xStep = xStep;
        this.yStep = yStep;
    }

    @Override
    public boolean wrapped()
    {
        return currentCount > 0 && x == minX && y == minY;
    }

    private boolean hasFilledX()
    {
        return currentCount % xStepCount == 0;
    }

    private boolean hasFilledY()
    {
        return currentCount % totalStepCount == 0;
    }

    @Override
    public PointValue next( SplittableRandom rng )
    {
        PointValue point = pointValue( crs, x, y );
        currentCount++;
        if ( hasFilledX() )
        {
            x = minX;
            if ( hasFilledY() )
            {
                y = minY;
            }
            else
            {
                y += yStep;
            }
        }
        else
        {
            x += xStep;
        }
        return point;
    }

    @Override
    public Value nextValue( SplittableRandom rng )
    {
        return next( rng );
    }
}
