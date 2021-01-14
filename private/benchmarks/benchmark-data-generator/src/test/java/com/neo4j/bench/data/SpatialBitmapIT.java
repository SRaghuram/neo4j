/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.data;

import com.neo4j.bench.data.PointGenerator.ClusterGridDefinition;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.file.Path;
import java.util.SplittableRandom;

import org.neo4j.graphdb.spatial.Point;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SkipThreadLeakageGuard;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.data.PointGenerator.ClusterGridDefinition.from;
import static com.neo4j.bench.data.PointGenerator.circleGrid;
import static com.neo4j.bench.data.PointGenerator.clusterGrid;
import static com.neo4j.bench.data.PointGenerator.xFor;
import static com.neo4j.bench.data.PointGenerator.yFor;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.round;

@SkipThreadLeakageGuard
@TestDirectoryExtension
public class SpatialBitmapIT
{
    private static final Logger LOG = LoggerFactory.getLogger( SpatialBitmapIT.class );

    @Inject
    public TestDirectory temporaryFolder;

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
        SplittableRandom rng = SplittableRandomProvider.newRandom( 42 );
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
        Path path = temporaryFolder.absolutePath().resolve( filename );
        LOG.debug( "Writing image to: " + path.toAbsolutePath() );
        spatialBitmap.writeTo( path );
    }
}
