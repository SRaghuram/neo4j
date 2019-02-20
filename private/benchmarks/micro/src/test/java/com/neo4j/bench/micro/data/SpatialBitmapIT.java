/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.data.PointGenerator.ClusterGridDefinition;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.file.Path;
import java.util.SplittableRandom;

import org.neo4j.graphdb.spatial.Point;

import static com.neo4j.bench.micro.data.PointGenerator.circleGrid;
import static com.neo4j.bench.micro.data.PointGenerator.clusterGrid;
import static com.neo4j.bench.micro.data.PointGenerator.xFor;
import static com.neo4j.bench.micro.data.PointGenerator.yFor;
import static com.neo4j.bench.micro.data.PointGenerator.ClusterGridDefinition.from;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.round;

public class SpatialBitmapIT
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void shouldMakePrettyBitmap() throws IOException
    {
        int extentMinX = -100000;
        int extentMaxX = 100000;
        int extentMinY = -100000;
        int extentMaxY = 100000;
        double clusterSizeX = (extentMaxX - extentMinX) * 0.1;
        double clusterSizeY = (extentMaxY - extentMinY) * 0.1;
        int clusterCountX = 9;
        int clusterCountY = 9;
        int pointCount = 10_000_000;
        ClusterGridDefinition definition = from(
                clusterSizeX,
                clusterSizeY,
                clusterCountX,
                clusterCountY,
                extentMinX,
                extentMaxX,
                extentMinY,
                extentMaxY,
                pointCount,
                new CRS.Cartesian() );
        doShouldMakePrettyBitmap( definition );
    }

    private void doShouldMakePrettyBitmap( ClusterGridDefinition definition ) throws IOException
    {
        doShouldMakePrettyBitmap( definition, clusterGrid( definition ), "distribution-clusters.png" );
        doShouldMakePrettyBitmap( definition, circleGrid( definition ), "distribution-circles.png" );
    }

    private void doShouldMakePrettyBitmap(
            ClusterGridDefinition definition,
            ValueGeneratorFactory<Point> generator,
            String filename ) throws IOException
    {
        SplittableRandom rng = RNGState.newRandom( 42 );
        ValueGeneratorFun<Point> fun = generator.create();
        long minX = Long.MAX_VALUE;
        long maxX = Long.MIN_VALUE;
        long minY = Long.MAX_VALUE;
        long maxY = Long.MIN_VALUE;
        SpatialBitmap spatialBitmap = SpatialBitmap.createFor(
                new BufferedImage( 1000, 1000, BufferedImage.TYPE_INT_RGB ),
                (int) round( definition.extentMinX() ),
                (int) round( definition.extentMaxX() ),
                (int) round( definition.extentMinY() ),
                (int) round( definition.extentMaxY() ) );
        do
        {
            Point point = fun.next( rng );
            double pointX = xFor( point );
            double pointY = yFor( point );
            minX = round( min( minX, pointX ) );
            maxX = round( max( maxX, pointX ) );
            minY = round( min( minY, pointY ) );
            maxY = round( max( maxY, pointY ) );
            spatialBitmap.addPointToBitmap( pointX, pointY );
        }
        while ( !fun.wrapped() );
        Path path = temporaryFolder.newFile( filename ).toPath();
        System.out.println( "Writing image to: " + path.toFile().getAbsolutePath() );
        spatialBitmap.writeTo( path );
    }
}
