/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.data;

import com.neo4j.bench.common.util.RichRandom;
import com.neo4j.bench.data.CRS.Cartesian;
import com.neo4j.bench.data.CRS.WGS84;
import com.neo4j.bench.data.PointGenerator.ClusterGridDefinition;
import org.junit.jupiter.api.Test;

import java.util.SplittableRandom;

import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.PointValue;

import static com.neo4j.bench.data.PointGenerator.ClusterGridDefinition.from;
import static com.neo4j.bench.data.PointGenerator.circleGrid;
import static com.neo4j.bench.data.PointGenerator.clusterGrid;
import static com.neo4j.bench.data.PointGenerator.grid;
import static com.neo4j.bench.data.PointGenerator.random;
import static com.neo4j.bench.data.PointGenerator.xFor;
import static com.neo4j.bench.data.PointGenerator.yFor;
import static java.lang.Math.round;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.values.storable.Values.pointValue;

public class PointGeneratorIT
{
    private double WGS_MIN_Y = -90;
    private double WGS_MAX_Y = 90;
    private double WGS_MIN_X = -180;
    private double WGS_MAX_X = 180;

    @Test
    public void shouldGenerateSmallGrid()
    {
        doShouldGenerateSmallPositiveGrid( new Cartesian() );
        doShouldGenerateSmallPositiveGrid( new WGS84() );
    }

    private void doShouldGenerateSmallPositiveGrid( CRS crs )
    {
        SplittableRandom rng = SplittableRandomProvider.newRandom( 42L );
        ValueGeneratorFactory<Point> gridGenerator = grid( -3, 6, -3, 6, 9, crs );
        ValueGeneratorFun<Point> gridFun = gridGenerator.create();
        assertThat( gridFun.next( rng ), equalTo( pointValue( crs.crs(), -3, -3 ) ) );
        assertFalse( gridFun.wrapped() );
        assertThat( gridFun.next( rng ), equalTo( pointValue( crs.crs(), 0, -3 ) ) );
        assertFalse( gridFun.wrapped() );
        assertThat( gridFun.next( rng ), equalTo( pointValue( crs.crs(), 3, -3 ) ) );
        assertFalse( gridFun.wrapped() );

        assertThat( gridFun.next( rng ), equalTo( pointValue( crs.crs(), -3, 0 ) ) );
        assertFalse( gridFun.wrapped() );
        assertThat( gridFun.next( rng ), equalTo( pointValue( crs.crs(), 0, 0 ) ) );
        assertFalse( gridFun.wrapped() );
        assertThat( gridFun.next( rng ), equalTo( pointValue( crs.crs(), 3, 0 ) ) );
        assertFalse( gridFun.wrapped() );

        assertThat( gridFun.next( rng ), equalTo( pointValue( crs.crs(), -3, 3 ) ) );
        assertFalse( gridFun.wrapped() );
        assertThat( gridFun.next( rng ), equalTo( pointValue( crs.crs(), 0, 3 ) ) );
        assertFalse( gridFun.wrapped() );
        assertThat( gridFun.next( rng ), equalTo( pointValue( crs.crs(), 3, 3 ) ) );
        assertTrue( gridFun.wrapped() ); // <-------- wrap!

        assertThat( gridFun.next( rng ), equalTo( pointValue( crs.crs(), -3, -3 ) ) );
        assertFalse( gridFun.wrapped() );
        assertThat( gridFun.next( rng ), equalTo( pointValue( crs.crs(), 0, -3 ) ) );
        assertFalse( gridFun.wrapped() );
    }

    @Test
    public void shouldGenerateLargeAsymmetricGrid()
    {
        RichRandom rng = new RichRandom( SplittableRandomProvider.newRandom( 42L ) );
        for ( int i = 0; i < 10; i++ )
        {
            double xExtent = 1_000_000 * rng.nextDouble();
            double yExtent = 1_000_000 * rng.nextDouble();
            double minX = rng.nextGaussian() * xExtent;
            double minY = rng.nextGaussian() * yExtent;
            double maxX = minX + xExtent;
            double maxY = minY + yExtent;
            doShouldGenerateLargeAsymmetricGrid(
                    new Cartesian(),
                    minX,
                    maxX,
                    minY,
                    maxY );

            // Why this is needed: Values for latitude should always lie in the interval [-90, 90], any other value outside this range will throw an exception.
            // Values for longitude should always lie in the interval [-180, 180],
            // any other value outside this range will now be wrapped around to fit in this range.
            double[] newMinYAndMaxY = restrictYCoordinateWithWGS84BoundingBox( minY, maxY );
            doShouldGenerateLargeAsymmetricGrid(
                    new WGS84(),
                    minX,
                    maxX,
                    newMinYAndMaxY[0],
                    newMinYAndMaxY[1] );
        }
    }

    private double[] restrictYCoordinateWithWGS84BoundingBox( double min, double max )
    {
        double a = wgsBoundingBoxForY( min );
        double b = wgsBoundingBoxForY( max );
        if ( a <= b )
        {
            return new double[]{a, b};
        }
        else
        {
            return new double[]{b, a}; // making sure that the order is always [min, max]
        }
    }

    // Values for longitude (Y) should always lie in the interval [-180, 180],
    // any other value outside this range will be wrapped around to fit in this range by this function
    private double wgsBoundingBoxForY( double value )
    {
        if ( value > 0 )
        {
            return value % WGS_MAX_Y;
        }
        else
        {
            return value % WGS_MIN_Y;
        }
    }

    private void doShouldGenerateLargeAsymmetricGrid(
            CRS crs,
            double minX,
            double maxX,
            double minY,
            double maxY )
    {
        int approximateCount = 100_000;

        // because in WGS-84 we wrap around the globe for anything that is not in [-180,180]
        if ( crs.crs().isGeographic() )
        {
            PointValue wrapMinPoint = pointValue( crs.crs(), minX, minY );
            PointValue wrapMaxPoint = pointValue( crs.crs(), maxX, minY ); // don't care for Y here, only doing it to wrap the X value
            // getting the real value after wrapping and making sure they are still sensible
            minX = wrapMinPoint.coordinate()[0];
            maxX = wrapMaxPoint.coordinate()[0];
            if ( minX > maxX )
            {
                // due to wrapping min and max must switch now
                double temp = minX;
                minX = maxX;
                maxX = temp;
            }
            assert minX < maxX;
        }
        ValueGeneratorFactory<Point> gridGenerator = grid( minX, maxX, minY, maxY, approximateCount, crs );
        ValueGeneratorFun<Point> gridFun1 = gridGenerator.create();
        ValueGeneratorFun<Point> gridFun2 = gridGenerator.create();

        PointValue firstPoint = pointValue( crs.crs(), minX, minY );
        Point previousPoint = gridFun1.next( null );
        assertThat( gridFun2.nextValue( null ), equalTo( previousPoint ) );
        assertThat( previousPoint, equalTo( firstPoint ) );
        assertFalse( gridFun1.wrapped() );
        assertFalse( gridFun2.wrapped() );

        Point point = gridFun1.next( null );
        assertThat( gridFun2.nextValue( null ), equalTo( point ) );
        assertFalse( gridFun1.wrapped() );

        int actualCount = 1;
        while ( !gridFun1.wrapped() )
        {
            assertThat( point, not( equalTo( previousPoint ) ) );
            assertThat( xFor( point ), either( greaterThanOrEqualTo( xFor( previousPoint ) ) ).or( equalTo( minX ) ) );
            assertThat( xFor( point ), allOf( greaterThanOrEqualTo( minX ), lessThanOrEqualTo( maxX ) ) );
            assertThat( yFor( point ), greaterThanOrEqualTo( yFor( previousPoint ) ) );
            assertThat( yFor( point ), allOf( greaterThanOrEqualTo( minY ), lessThanOrEqualTo( maxY ) ) );
            previousPoint = point;
            actualCount++;
            point = gridFun1.next( null );
            assertThat( gridFun2.nextValue( null ), equalTo( point ) );
        }
        assertThat( actualCount, allOf(
                greaterThanOrEqualTo( (int) round( approximateCount * 0.9 ) ),
                lessThanOrEqualTo( approximateCount ) ) );
        // calling wrapped twice gives same result
        assertTrue( gridFun1.wrapped() );
        // last point, created just before wrap
        assertThat( point, not( equalTo( firstPoint ) ) );
        point = gridFun1.next( null );
        // first point again
        assertThat( point, equalTo( firstPoint ) );
        assertThat( gridFun2.nextValue( null ), equalTo( point ) );
        assertFalse( gridFun1.wrapped() );
    }

    @Test
    public void shouldGenerateLargeAsymmetricRandom()
    {
        RichRandom rng = new RichRandom( SplittableRandomProvider.newRandom( 42L ) );
        RichRandom rng1 = new RichRandom( SplittableRandomProvider.newRandom( 42L ) );
        RichRandom rng2 = new RichRandom( SplittableRandomProvider.newRandom( 42L ) );

        for ( int i = 0; i < 10; i++ )
        {
            double xExtent = 1_000_000 * rng.nextDouble();
            double yExtent = 1_000_000 * rng.nextDouble();
            double minX = rng.nextGaussian() * xExtent;
            double minY = rng.nextGaussian() * yExtent;
            double maxX = minX + xExtent;
            double maxY = minY + yExtent;
            doShouldGenerateLargeAsymmetricRandom(
                    new Cartesian(),
                    minX,
                    maxX,
                    minY,
                    maxY,
                    rng1.innerRng(),
                    rng2.innerRng() );
            double[] newMinYAndMaxY = restrictYCoordinateWithWGS84BoundingBox( minY, maxY );

            doShouldGenerateLargeAsymmetricRandom(
                    new WGS84(),
                    minX,
                    maxX,
                    newMinYAndMaxY[0],
                    newMinYAndMaxY[1],
                    rng1.innerRng(),
                    rng2.innerRng() );
        }
    }

    private void doShouldGenerateLargeAsymmetricRandom(
            CRS crs,
            double minX,
            double maxX,
            double minY,
            double maxY,
            SplittableRandom rng1,
            SplittableRandom rng2 )
    {
        // because in WGS-84 we wrap around the globe for anything that is not in [-180,180]
        if ( crs.crs().isGeographic() )
        {
            // getting the real value after wrapping and making sure they are still sensible
            PointValue wrapMinPoint = pointValue( crs.crs(), minX, minY );
            PointValue wrapMaxPoint = pointValue( crs.crs(), maxX, minY ); // don't care for Y here, only doing it to wrap the X value
            minX = wrapMinPoint.coordinate()[0];
            maxX = wrapMaxPoint.coordinate()[0];
            if ( minX > maxX )
            {
                // due to wrapping min and max must switch now
                double temp = minX;
                minX = maxX;
                maxX = temp;
            }
            assert minX < maxX;
        }

        ValueGeneratorFactory<Point> randomGenerator = random( minX, maxX, minY, maxY, crs );
        ValueGeneratorFun<Point> randomFun1 = randomGenerator.create();
        ValueGeneratorFun<Point> randomFun2 = randomGenerator.create();

        for ( int i = 0; i < 100_000; i++ )
        {
            Point point = randomFun1.next( rng1 );
            assertThat( randomFun2.nextValue( rng2 ), equalTo( point ) );
            assertThat( xFor( point ), allOf( greaterThanOrEqualTo( minX ), lessThanOrEqualTo( maxX ) ) );
            assertThat( yFor( point ), allOf( greaterThanOrEqualTo( minY ), lessThanOrEqualTo( maxY ) ) );
            assertFalse( randomFun1.wrapped() );
        }

        // TODO also test distribution?
    }

    @Test
    public void shouldCreateCorrectClusterGridDefinition()
    {
        double expectedClusterSizeX = 3;
        double expectedClusterSizeY = 3;
        double expectedExtentMinX = 0;
        double expectedExtentMaxX = 10;
        double expectedExtentMinY = 0;
        double expectedExtentMaxY = 10;
        long approximatePointCount = 81;
        Cartesian expectedCrs = new Cartesian();

        ClusterGridDefinition definition = from(
                expectedClusterSizeX,
                expectedClusterSizeY,
                expectedExtentMinX,
                expectedExtentMaxX,
                expectedExtentMinY,
                expectedExtentMaxY,
                approximatePointCount,
                expectedCrs );

        // space between neighbor clusters, and between clusters and extent borders
        double expectedClusterGapSizeX =
                (definition.extentX() - expectedClusterSizeX * definition.clusterCountX()) /
                (definition.clusterCountX() + 1);
        double expectedClusterGapSizeY =
                (definition.extentY() - expectedClusterSizeY * definition.clusterCountY()) /
                (definition.clusterCountY() + 1);

        assertThat( definition.crs(), equalTo( expectedCrs ) );
        assertThat( definition.extentMinX(), equalTo( 0D ) );
        assertThat( definition.extentMaxX(), equalTo( 10D ) );
        assertThat( definition.extentMinY(), equalTo( 0D ) );
        assertThat( definition.extentMaxY(), equalTo( 10D ) );
        // in this case approximate point count should equal point could because it divides cleanly into cluster count
        assertThat( definition.pointCount(), equalTo( approximatePointCount ) );
        assertThat( definition.clusterSizeX(), equalTo( 3D ) );
        assertThat( definition.clusterSizeY(), equalTo( 3D ) );
        assertThat( definition.extentX(), equalTo( expectedExtentMaxX - expectedExtentMinX ) );
        assertThat( definition.extentY(), equalTo( expectedExtentMaxY - expectedExtentMinY ) );
        assertThat( definition.clusterCountX(), equalTo( 3L ) );
        assertThat( definition.clusterCountY(), equalTo( 3L ) );
        assertThat( definition.pointsPerCluster(), equalTo( 9L ) );
        assertThat( definition.clusterGapSizeX(), equalTo( expectedClusterGapSizeX ) );
        assertThat( definition.clusterGapSizeY(), equalTo( expectedClusterGapSizeY ) );
    }

    @Test
    public void shouldGenerateSmallClusteredGrid()
    {
        doShouldGenerateSmallClusteredGrid( new Cartesian() );
        doShouldGenerateSmallClusteredGrid( new WGS84() );
    }

    private void doShouldGenerateSmallClusteredGrid( CRS crs )
    {
        SplittableRandom rng1 = SplittableRandomProvider.newRandom( 42L );
        SplittableRandom rng2 = SplittableRandomProvider.newRandom( 42L );
        double clusterSizeX = 3;
        double clusterSizeY = 3;
        double extentMinX = 0;
        double extentMaxX = 10;
        double extentMinY = 0;
        double extentMaxY = 10;
        int pointCount = 81;

        ClusterGridDefinition definition = from(
                clusterSizeX,
                clusterSizeY,
                extentMinX,
                extentMaxX,
                extentMinY,
                extentMaxY,
                pointCount,
                crs );
        ValueGeneratorFactory<Point> clusterGenerator = clusterGrid( definition );
        ValueGeneratorFun<Point> clusterFun1 = clusterGenerator.create();
        ValueGeneratorFun<Point> clusterFun2 = clusterGenerator.create();
        ValueGeneratorFun<Point> clusterCenters = definition.clusterCenters();
        Point clusterCenter = clusterCenters.next( rng1 );
        double clusterCenterX = xFor( clusterCenter );
        double clusterCenterY = yFor( clusterCenter );

        assertFalse( clusterFun1.wrapped() );
        long actualCount = 0;
        PointValue firstPoint = pointValue( crs.crs(), 1.75, 1.75 );
        Point point = clusterFun1.next( rng1 );
        assertThat( clusterFun2.nextValue( rng2 ), equalTo( point ) );
        actualCount++;
        assertThat( point, equalTo( firstPoint ) );
        assertThat( point, equalTo( clusterCenter ) );
        do
        {
            point = clusterFun1.next( rng1 );
            assertThat( clusterFun2.nextValue( rng2 ), equalTo( point ) );
            actualCount++;
            double x = xFor( point );
            double y = yFor( point );

            assertThat( x, allOf( greaterThanOrEqualTo( extentMinX ), lessThanOrEqualTo( extentMaxX ) ) );
            assertThat( y, allOf( greaterThanOrEqualTo( extentMinY ), lessThanOrEqualTo( extentMaxY ) ) );

            // point is within current cluster bounds
            assertThat( x,
                        allOf(
                                greaterThanOrEqualTo( clusterCenterX - definition.clusterSizeX() / 2 ),
                                lessThanOrEqualTo( clusterCenterX + definition.clusterSizeX() / 2 ) ) );
            assertThat( y, allOf(
                    greaterThanOrEqualTo( clusterCenterY - definition.clusterSizeY() / 2 ),
                    lessThanOrEqualTo( clusterCenterY + definition.clusterSizeY() / 2 ) ) );

            if ( actualCount % definition.pointsPerCluster() == 0 && !clusterFun1.wrapped() )
            {
                // at last point of current cluster

                clusterCenter = clusterCenters.next( rng1 );
                clusterCenterX = xFor( clusterCenter );
                clusterCenterY = yFor( clusterCenter );

                point = clusterFun1.next( rng1 );
                assertThat( clusterFun2.nextValue( rng2 ), equalTo( point ) );
                actualCount++;
                x = xFor( point );
                y = yFor( point );
                assertThat( point, equalTo( clusterCenter ) );

                // cluster center is sufficient distance from extent borders
                assertThat( x, allOf(
                        greaterThanOrEqualTo( definition.extentMinX() + definition.clusterSizeX() / 2 ),
                        lessThanOrEqualTo( definition.extentMaxX() - definition.clusterSizeX() / 2 ) ) );
                assertThat( y, allOf(
                        greaterThanOrEqualTo( definition.extentMinY() + definition.clusterSizeY() / 2 ),
                        lessThanOrEqualTo( definition.extentMaxY() - definition.clusterSizeY() / 2 ) ) );
            }
        }
        while ( !clusterFun1.wrapped() );
        assertThat( actualCount, equalTo( definition.pointCount() ) );
        long expectedPointCount =
                definition.pointsPerCluster() * definition.clusterCountX() * definition.clusterCountY();
        assertThat( actualCount, equalTo( expectedPointCount ) );
        // calling wrapped twice gives same result
        assertTrue( clusterFun1.wrapped() );
        // last point, created just before wrap
        assertThat( point, not( equalTo( firstPoint ) ) );
        // first point again
        point = clusterFun1.next( rng1 );
        assertThat( clusterFun2.nextValue( rng2 ), equalTo( point ) );
        assertThat( point, equalTo( firstPoint ) );
        assertFalse( clusterFun1.wrapped() );
    }

    @Test
    public void shouldGenerateLargeClusteredGrid()
    {
        RichRandom rng = new RichRandom( SplittableRandomProvider.newRandom( 42L ) );
        RichRandom rng1 = new RichRandom( SplittableRandomProvider.newRandom( 42L ) );
        RichRandom rng2 = new RichRandom( SplittableRandomProvider.newRandom( 42L ) );

        // specific scenario that is easy to reason about and surfaced many bugs during development
        int approximateCount = 100_000;
        double clusterSizeX = 50;
        double clusterSizeY = 50;
        double extentMinX = WGS_MIN_X;
        double extentMaxX = WGS_MAX_X;
        double extentMinY = WGS_MIN_Y;
        double extentMaxY = WGS_MAX_Y;

        doShouldGenerateLargeClusteredGrid(
                from(
                        clusterSizeX,
                        clusterSizeY,
                        extentMinX,
                        extentMaxX,
                        extentMinY,
                        extentMaxY,
                        approximateCount,
                        new Cartesian() ),
                rng1.innerRng(),
                rng2.innerRng() );
        doShouldGenerateLargeClusteredGrid(
                from(
                        clusterSizeX,
                        clusterSizeY,
                        extentMinX,
                        extentMaxX,
                        extentMinY,
                        extentMaxY,
                        approximateCount,
                        new WGS84() ),
                rng1.innerRng(),
                rng2.innerRng() );

        // specific scenario that is easy to reason about and surfaced many bugs during development
        clusterSizeX = 100;
        clusterSizeY = 100;
        extentMinX = WGS_MIN_X;
        extentMaxX = WGS_MAX_X;
        extentMinY = WGS_MIN_Y;
        extentMaxY = WGS_MAX_Y;
        doShouldGenerateLargeClusteredGrid(
                from(
                        clusterSizeX,
                        clusterSizeY,
                        extentMinX,
                        extentMaxX,
                        extentMinY,
                        extentMaxY,
                        approximateCount,
                        new Cartesian() ),
                rng1.innerRng(),
                rng2.innerRng() );
        doShouldGenerateLargeClusteredGrid(
                from(
                        clusterSizeX,
                        clusterSizeY,
                        extentMinX,
                        extentMaxX,
                        extentMinY,
                        extentMaxY,
                        approximateCount,
                        new WGS84() ),
                rng1.innerRng(),
                rng2.innerRng() );

        // random valid scenarios
        for ( int i = 0; i < 10; i++ )
        {
            double extentX = WGS_MAX_X * rng.nextDouble();
            double extentY = WGS_MAX_Y * rng.nextDouble();
            // making sure that the range for min can fit all of the extent to not hit the upper boundary
            extentMinX = rng.nextDouble() * getOneOrMinusOne( rng ) * (WGS_MAX_X - extentX);
            extentMaxX = extentMinX + extentX;
            extentMinY = rng.nextDouble() * getOneOrMinusOne( rng ) * (WGS_MAX_Y - extentY);
            extentMaxY = extentMinY + extentY;
            // it will always be possible to fit at least 1 cluster in the extent
            double maxPercentageOfExtentThatClusterCanBe = 0.5;
            clusterSizeX = rng.nextDouble() * maxPercentageOfExtentThatClusterCanBe * extentX;
            clusterSizeY = rng.nextDouble() * maxPercentageOfExtentThatClusterCanBe * extentY;
            doShouldGenerateLargeClusteredGrid(
                    from(
                            clusterSizeX,
                            clusterSizeY,
                            extentMinX,
                            extentMaxX,
                            extentMinY,
                            extentMaxY,
                            approximateCount,
                            new Cartesian() ),
                    rng1.innerRng(),
                    rng2.innerRng() );
            doShouldGenerateLargeClusteredGrid(
                    from(
                            clusterSizeX,
                            clusterSizeY,
                            extentMinX,
                            extentMaxX,
                            extentMinY,
                            extentMaxY,
                            approximateCount,
                            new WGS84() ),
                    rng1.innerRng(),
                    rng2.innerRng() );
        }
    }

    public double getOneOrMinusOne( RichRandom rng )
    {
        switch ( rng.nextInt( 0, 2 ) )
        {
        case 0:
            return -1;
        default:
            return 1;
        }
    }

    public double wrapXValueInWGS84Boundary( double x )
    {
        return pointValue( new WGS84().crs(), x, 0 ).coordinate()[0];
    }

    private void doShouldGenerateLargeClusteredGrid( ClusterGridDefinition definition, SplittableRandom rng1, SplittableRandom rng2 )
    {
        ValueGeneratorFun<Point> clusterCenters = definition.clusterCenters();
        Point clusterCenter = clusterCenters.next( rng1 );
        double clusterCenterX = xFor( clusterCenter );
        double clusterCenterY = yFor( clusterCenter );

        ValueGeneratorFactory<Point> clusterGenerator = clusterGrid( definition );
        ValueGeneratorFun<Point> clusterFun1 = clusterGenerator.create();
        ValueGeneratorFun<Point> clusterFun2 = clusterGenerator.create();
        assertFalse( clusterFun1.wrapped() );
        long actualCount = 0;

        Point firstPoint = clusterFun1.next( rng1 );
        assertThat( clusterFun2.nextValue( rng2 ), equalTo( firstPoint ) );
        actualCount++;
        assertThat( firstPoint, equalTo( clusterCenter ) );
        // cluster center is sufficient distance from extent border
        assertThat( xFor( firstPoint ), allOf(
                greaterThanOrEqualTo( definition.extentMinX() + definition.clusterSizeX() / 2 ),
                lessThanOrEqualTo( definition.extentMaxX() - definition.clusterSizeX() / 2 ) ) );
        assertThat( yFor( firstPoint ), allOf(
                greaterThanOrEqualTo( definition.extentMinY() + definition.clusterSizeY() / 2 ),
                lessThanOrEqualTo( definition.extentMaxY() - definition.clusterSizeY() / 2 ) ) );
        assertFalse( clusterFun1.wrapped() );
        Point point;
        do
        {
            point = clusterFun1.next( rng1 );
            assertThat( clusterFun2.nextValue( rng2 ), equalTo( point ) );
            actualCount++;
            double x = xFor( point );
            double y = yFor( point );

            assertThat( x, allOf(
                    greaterThanOrEqualTo( definition.extentMinX() ),
                    lessThanOrEqualTo( definition.extentMaxX() ) ) );
            assertThat( y, allOf(
                    greaterThanOrEqualTo( definition.extentMinY() ),
                    lessThanOrEqualTo( definition.extentMaxY() ) ) );

            assertThat( x, allOf(
                    greaterThanOrEqualTo( clusterCenterX - definition.clusterSizeX() / 2 ),
                    lessThanOrEqualTo( clusterCenterX + definition.clusterSizeX() / 2 ) ) );
            assertThat( y, allOf(
                    greaterThanOrEqualTo( clusterCenterY - definition.clusterSizeY() / 2 ),
                    lessThanOrEqualTo( clusterCenterY + definition.clusterSizeY() / 2 ) ) );

            if ( actualCount % definition.pointsPerCluster() == 0 && !clusterFun1.wrapped() )
            {
                // current point is last point of current cluster

                clusterCenter = clusterCenters.next( rng1 );
                clusterCenterX = xFor( clusterCenter );
                clusterCenterY = yFor( clusterCenter );

                point = clusterFun1.next( rng1 );
                assertThat( clusterFun2.nextValue( rng2 ), equalTo( point ) );
                actualCount++;
                assertThat( point, equalTo( clusterCenter ) );

                assertThat( xFor( point ), allOf(
                        greaterThanOrEqualTo( definition.extentMinX() + definition.clusterSizeX() / 2 ),
                        lessThanOrEqualTo( definition.extentMaxX() - definition.clusterSizeX() / 2 ) ) );
                assertThat( yFor( point ), allOf(
                        greaterThanOrEqualTo( definition.extentMinY() + definition.clusterSizeY() / 2 ),
                        lessThanOrEqualTo( definition.extentMaxY() - definition.clusterSizeY() / 2 ) ) );
            }
        }
        while ( !clusterFun1.wrapped() );
        assertThat( actualCount, equalTo( definition.pointCount() ) );
        long expectedPointCount =
                definition.pointsPerCluster() * definition.clusterCountX() * definition.clusterCountY();
        assertThat( actualCount, equalTo( expectedPointCount ) );

        // calling wrapped twice gives same result
        assertTrue( clusterFun1.wrapped() );
        // last point, created just before wrap
        assertThat( point, not( equalTo( firstPoint ) ) );
        // first point again
        point = clusterFun1.next( rng1 );
        assertThat( clusterFun2.nextValue( rng2 ), equalTo( point ) );
        assertThat( point, equalTo( firstPoint ) );
        assertFalse( clusterFun1.wrapped() );
    }

    @Test
    public void shouldGenerateLargeCircleGrid()
    {
        RichRandom rng = new RichRandom( SplittableRandomProvider.newRandom( 42L ) );
        RichRandom rng1 = new RichRandom( SplittableRandomProvider.newRandom( 42L ) );
        RichRandom rng2 = new RichRandom( SplittableRandomProvider.newRandom( 42L ) );

        // specific scenario that is easy to reason about and surfaced many bugs during development
        int approximateCount = 100_000;
        double circleRadius;
        long circleCountX;
        long circleCountY;
        double extentMinX;
        double extentMaxX;
        double extentMinY;
        double extentMaxY;

        // specific scenario that is easy to reason about and surfaced many bugs during development
        circleRadius = 2000;
        circleCountX = 1;
        circleCountY = 1;
        extentMinX = -1000;
        extentMaxX = 1000;
        extentMinY = -1000;
        extentMaxY = 1000;
        doShouldGenerateLargeCircleGrid(
                from(
                        circleRadius,
                        circleRadius,
                        circleCountX,
                        circleCountY,
                        extentMinX,
                        extentMaxX,
                        extentMinY,
                        extentMaxY,
                        approximateCount,
                        new Cartesian() ),
                rng1.innerRng(),
                rng2.innerRng() );
        doShouldGenerateLargeCircleGrid(
                from(
                        WGS_MAX_Y * 2,
                        WGS_MAX_Y * 2,
                        circleCountX,
                        circleCountY,
                        WGS_MIN_Y,
                        WGS_MAX_Y,
                        WGS_MIN_Y,
                        WGS_MAX_Y,
                        approximateCount,
                        new WGS84() ),
                rng1.innerRng(),
                rng2.innerRng() );

        // random valid scenarios
        for ( int i = 0; i < 10; i++ )
        {
            double extentX = 1_000_000 * rng.nextDouble();
            double extentY = 1_000_000 * rng.nextDouble();
            extentMinX = rng.nextGaussian() * extentX;
            extentMaxX = extentMinX + extentX;
            extentMinY = rng.nextGaussian() * extentY;
            extentMaxY = extentMinY + extentY;
            // it will always be possible to fit at least 1 cluster in the extent
            double maxPercentageOfExtentThatClusterCanBe = 0.5;
            circleRadius = rng.nextDouble() * maxPercentageOfExtentThatClusterCanBe * Math.min( extentX, extentY );
            doShouldGenerateLargeClusteredGrid(
                    from(
                            circleRadius,
                            circleRadius,
                            circleCountX,
                            circleCountY,
                            extentMinX,
                            extentMaxX,
                            extentMinY,
                            extentMaxY,
                            approximateCount,
                            new Cartesian() ),
                    rng1.innerRng(),
                    rng2.innerRng() );
            double[] newMinYAndMaxY = restrictYCoordinateWithWGS84BoundingBox( extentMinY, extentMaxY );
            double newExtentMinY = newMinYAndMaxY[0];
            double newExtentMaxY = newMinYAndMaxY[1];
            double newExtend = newExtentMaxY - newExtentMinY;

            doShouldGenerateLargeClusteredGrid(
                    from(
                            newExtend,
                            newExtend,
                            circleCountX,
                            circleCountY,
                            newExtentMinY,
                            newExtentMaxY,
                            newExtentMinY,
                            newExtentMaxY,
                            approximateCount,
                            new WGS84() ),
                    rng1.innerRng(),
                    rng2.innerRng() );
        }
    }

    private void doShouldGenerateLargeCircleGrid( ClusterGridDefinition definition, SplittableRandom rng1, SplittableRandom rng2 )
    {
        ValueGeneratorFactory<Point> clusterGenerator = circleGrid( definition );

        ValueGeneratorFun<Point> circleCenters1 = definition.clusterCenters();
        ValueGeneratorFun<Point> circleCenters2 = definition.clusterCenters();

        ValueGeneratorFun<Point> circleFun1 = clusterGenerator.create();
        ValueGeneratorFun<Point> circleFun2 = clusterGenerator.create();
        assertFalse( circleFun1.wrapped() );
        long actualCount = 0;
        Point circleCenter = circleCenters1.next( rng1 );
        assertThat( circleCenters2.nextValue( rng2 ), equalTo( circleCenter ) );
        double circleCenterX = xFor( circleCenter );
        double circleCenterY = yFor( circleCenter );
        Point point;
        do
        {
            point = circleFun1.next( rng1 );
            assertThat( circleFun2.nextValue( rng2 ), equalTo( point ) );
            actualCount++;
            double x = xFor( point );
            double y = yFor( point );

            assertThat( x, allOf(
                    greaterThanOrEqualTo( definition.extentMinX() ),
                    lessThanOrEqualTo( definition.extentMaxX() ) ) );
            assertThat( y, allOf(
                    greaterThanOrEqualTo( definition.extentMinY() ),
                    lessThanOrEqualTo( definition.extentMaxY() ) ) );

            assertThat( x, allOf(
                    greaterThanOrEqualTo( circleCenterX - definition.clusterSizeX() / 2 ),
                    lessThanOrEqualTo( circleCenterX + definition.clusterSizeX() / 2 ) ) );
            assertThat( y, allOf(
                    greaterThanOrEqualTo( circleCenterY - definition.clusterSizeY() / 2 ),
                    lessThanOrEqualTo( circleCenterY + definition.clusterSizeY() / 2 ) ) );
            if ( actualCount % definition.pointsPerCluster() == 0 )
            {
                circleCenter = circleCenters1.next( rng1 );
                assertThat( circleCenters2.nextValue( rng2 ), equalTo( circleCenter ) );
                circleCenterX = xFor( circleCenter );
                circleCenterY = yFor( circleCenter );
            }
        }
        while ( !circleFun1.wrapped() );
        assertThat( actualCount, equalTo( definition.pointCount() ) );
        long expectedPointCount =
                definition.pointsPerCluster() * definition.clusterCountX() * definition.clusterCountY();
        assertThat( actualCount, equalTo( expectedPointCount ) );

        // calling wrapped twice gives same result
        assertTrue( circleFun1.wrapped() );
        circleFun1.next( rng1 );
        circleFun2.nextValue( rng2 );
        assertFalse( circleFun1.wrapped() );
    }
}
