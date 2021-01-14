/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.data;

import com.neo4j.bench.common.util.RichRandom;
import com.neo4j.bench.common.util.RichRandom.GaussianState;

import java.util.Objects;
import java.util.SplittableRandom;

import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;

import static com.neo4j.bench.data.NumberGenerator.randDouble;
import static java.lang.Math.cos;
import static java.lang.Math.floor;
import static java.lang.Math.min;
import static java.lang.Math.round;
import static java.lang.Math.sin;
import static java.lang.Math.sqrt;
import static java.lang.String.format;
import static org.neo4j.values.storable.Values.pointValue;

public abstract class PointGenerator
{
    public static ValueGeneratorFactory<Point> diagonal( ValueGeneratorFactory<Double> valueGeneratorFactory, CRS crs )
    {
        return new DiagonalSpatialGeneratorFactory( valueGeneratorFactory, crs.crs() );
    }

    public static ValueGeneratorFactory<Point> grid(
            double minX,
            double maxX,
            double minY,
            double maxY,
            long approximateCount, // will only be exact when dimensions are perfectly divisible
            CRS crs )
    {
        // same number of steps for each dimension
        int stepsPerDimension = (int) round( sqrt( approximateCount ) );
        double extentX = maxX - minX;
        double extentY = maxY - minY;
        double xStep = extentX / stepsPerDimension;
        double yStep = extentY / stepsPerDimension;
        return new GridSpatialGeneratorFactory(
                minX,
                minY,
                stepsPerDimension,
                stepsPerDimension,
                xStep,
                yStep,
                crs.crs() );
    }

    public static ValueGeneratorFactory<Point> random(
            double minX,
            double maxX,
            double minY,
            double maxY,
            CRS crs )
    {
        return new RandomSpatialGeneratorFactory( minX, minY, maxX, maxY, crs.crs() );
    }

    public static ValueGeneratorFactory<Point> clusterGrid( ClusterGridDefinition definition )
    {
        return new ClusteredSpatialGeneratorFactory( definition );
    }

    public static ValueGeneratorFactory<Point> circleGrid( ClusterGridDefinition definition )
    {
        return new CirclesSpatialGeneratorFactory( definition );
    }

    public static class ClusterGridDefinition
    {
        private double clusterSizeX;
        private double clusterSizeY;
        private double extentMinX;
        private double extentMaxX;
        private double extentMinY;
        private double extentMaxY;
        private double extentX;
        private double extentY;
        private long pointCount;
        private String crsString;

        private long clusterCountX;
        private long clusterCountY;
        private long pointsPerCluster;

        private double clusterGapSizeX;
        private double clusterGapSizeY;

        private GridSpatialGeneratorFactory clusterCornersFactory;

        public static ClusterGridDefinition from(
                double clusterSizeX,
                double clusterSizeY,
                double extentMinX,
                double extentMaxX,
                double extentMinY,
                double extentMaxY,
                long approximatePointCount,
                CRS crs )
        {
            double extentX = extentMaxX - extentMinX;
            double extentY = extentMaxY - extentMinY;

            // number of clusters in x dimension
            long unsafeClusterCountX = round( floor( extentX / clusterSizeX ) );
            // there should never be more clusters than points -- empty clusters are meaningless
            long clusterCountX = min( round( sqrt( approximatePointCount ) ), unsafeClusterCountX );

            // number of clusters in y dimension
            long unsafeClusterCountY = round( floor( extentY / clusterSizeY ) );
            // there should never be more clusters than points -- empty clusters are meaningless
            long clusterCountY = min( round( sqrt( approximatePointCount ) ), unsafeClusterCountY );

            return new ClusterGridDefinition(
                    clusterSizeX,
                    clusterSizeY,
                    clusterCountX,
                    clusterCountY,
                    extentMinX,
                    extentMaxX,
                    extentMinY,
                    extentMaxY,
                    approximatePointCount,
                    crs );
        }

        public static ClusterGridDefinition from(
                double clusterSizeX,
                double clusterSizeY,
                long clusterCountX,
                long clusterCountY,
                double extentMinX,
                double extentMaxX,
                double extentMinY,
                double extentMaxY,
                long approximatePointCount,
                CRS crs )
        {
            return new ClusterGridDefinition(
                    clusterSizeX,
                    clusterSizeY,
                    clusterCountX,
                    clusterCountY,
                    extentMinX,
                    extentMaxX,
                    extentMinY,
                    extentMaxY,
                    approximatePointCount,
                    crs );
        }

        private static void assertValidClusterSize(
                double clusterSizeX,
                double clusterSizeY,
                long clusterCountX,
                long clusterCountY,
                double extentX,
                double extentY )
        {
            if ( clusterCountX <= 0 )
            {
                throw new RuntimeException( "Must be at least 1 cluster in x-dim, but was: " + clusterCountX );
            }
            if ( clusterCountY <= 0 )
            {
                throw new RuntimeException( "Must be at least 1 cluster in y-dim, but was: " + clusterCountY );
            }
            if ( clusterSizeX > extentX )
            {
                throw new RuntimeException( format(
                        "x-dim of cluster (%s) is larger than x-dim of total extent (%s)",
                        clusterSizeX, extentX ) );
            }
            if ( clusterSizeY > extentY )
            {
                throw new RuntimeException( format(
                        "y-dim= of cluster (%s) is larger than y-dim of total extent (%s)",
                        clusterSizeY, extentY ) );
            }
            if ( clusterSizeX * clusterCountX > extentX )
            {
                throw new RuntimeException( format(
                        "Combination of cluster x-dim & cluster count exceeds x-dim of total extent: %s * %s = %s > %s",
                        clusterSizeX, clusterCountX, clusterSizeX * clusterCountX, extentX ) );
            }
            if ( clusterSizeY * clusterCountY > extentY )
            {
                throw new RuntimeException( format(
                        "Combination of cluster y-dim & cluster count exceeds y-dim of total extent: %s * %s = %s > %s",
                        clusterSizeY, clusterCountY, clusterSizeY * clusterCountY, extentY ) );
            }
        }

        private ClusterGridDefinition()
        {

        }

        private ClusterGridDefinition(
                double clusterSizeX,
                double clusterSizeY,
                long clusterCountX,
                long clusterCountY,
                double extentMinX,
                double extentMaxX,
                double extentMinY,
                double extentMaxY,
                long approximatePointCount,
                CRS crs )
        {
            this.clusterSizeX = clusterSizeX;
            this.clusterSizeY = clusterSizeY;
            this.clusterCountX = clusterCountX;
            this.clusterCountY = clusterCountY;
            this.extentMinX = extentMinX;
            this.extentMaxX = extentMaxX;
            this.extentMinY = extentMinY;
            this.extentMaxY = extentMaxY;
            this.extentX = extentMaxX - extentMinX;
            this.extentY = extentMaxY - extentMinY;
            this.crsString = crs.name();
            this.pointsPerCluster = approximatePointCount / (clusterCountX * clusterCountY);
            this.pointCount = clusterCountX * clusterCountY * pointsPerCluster;

            double emptySpaceX = extentX - (clusterSizeX * clusterCountX);
            long gapCountX = clusterCountX + 1;
            this.clusterGapSizeX = emptySpaceX / gapCountX;

            double emptySpaceY = extentY - (clusterSizeY * clusterCountY);
            long gapCountY = clusterCountY + 1;
            this.clusterGapSizeY = emptySpaceY / gapCountY;

            double xClusterCenterStep = clusterSizeX + clusterGapSizeX;
            double yClusterCenterStep = clusterSizeY + clusterGapSizeY;
            double clusterCornersMinX = extentMinX + clusterGapSizeX;
            double clusterCornersMinY = extentMinY + clusterGapSizeY;
            this.clusterCornersFactory = new GridSpatialGeneratorFactory(
                    clusterCornersMinX,
                    clusterCornersMinY,
                    clusterCountX,
                    clusterCountY,
                    xClusterCenterStep,
                    yClusterCenterStep,
                    crs.crs() );

            assertValidClusterSize( clusterSizeX, clusterSizeY, clusterCountX, clusterCountY, extentX, extentY );
        }

        public CRS crs()
        {
            return CRS.from( crsString );
        }

        public double extentMinX()
        {
            return extentMinX;
        }

        public double extentMaxX()
        {
            return extentMaxX;
        }

        public double extentMinY()
        {
            return extentMinY;
        }

        public double extentMaxY()
        {
            return extentMaxY;
        }

        public long pointCount()
        {
            return pointCount;
        }

        public double clusterSizeX()
        {
            return clusterSizeX;
        }

        public double clusterSizeY()
        {
            return clusterSizeY;
        }

        public double extentX()
        {
            return extentX;
        }

        public double extentY()
        {
            return extentY;
        }

        public long clusterCountX()
        {
            return clusterCountX;
        }

        public long clusterCountY()
        {
            return clusterCountY;
        }

        public long pointsPerCluster()
        {
            return pointsPerCluster;
        }

        public double clusterGapSizeX()
        {
            return clusterGapSizeX;
        }

        public double clusterGapSizeY()
        {
            return clusterGapSizeY;
        }

        public ValueGeneratorFun<Point> clusterCenters()
        {
            return new ValueGeneratorFun<>()
            {
                private final double clusterDiameterX = clusterSizeX / 2;
                private final double clusterDiameterY = clusterSizeY / 2;
                private final CoordinateReferenceSystem crs = crs().crs();
                private final ValueGeneratorFun<Point> clusterCorners = clusterCornersFactory.create();

                @Override
                public boolean wrapped()
                {
                    return clusterCorners.wrapped();
                }

                @Override
                public PointValue next( SplittableRandom rng )
                {
                    Point clusterCorner;
                    double clusterCenterX;

                    do
                    {
                        clusterCorner = clusterCorners.next( rng );
                        clusterCenterX = xFor( clusterCorner ) + clusterDiameterX;
                    }
                    while ( clusterCenterX + clusterDiameterX >= 180 && crs.isGeographic() ); // prevent stuff from being wrapped by WGS-84 boundaries

                    double clusterCenterY = yFor( clusterCorner ) + clusterDiameterY;
                    return pointValue( crs, clusterCenterX, clusterCenterY );
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return next( rng );
                }
            };
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName() + ":\n" +
                    "\tclusterSizeX:     " + clusterSizeX + "\n" +
                    "\tclusterSizeY:     " + clusterSizeY + "\n" +
                    "\textentMinX:       " + extentMinX + "\n" +
                    "\textentMaxX:       " + extentMaxX + "\n" +
                    "\textentMinY:       " + extentMinY + "\n" +
                    "\textentMaxY:       " + extentMaxY + "\n" +
                    "\textentX:          " + extentX + "\n" +
                    "\textentY:          " + extentY + "\n" +
                    "\tpointCount:       " + pointCount + "\n" +
                    "\tcrsString:        " + crsString + "\n" +
                    "\tclusterCountX:    " + clusterCountX + "\n" +
                    "\tclusterCountY:    " + clusterCountY + "\n" +
                    "\tpointsPerCluster: " + pointsPerCluster + "\n" +
                    "\tclusterGapSizeX:  " + clusterGapSizeX + "\n" +
                    "\tclusterGapSizeY:  " + clusterGapSizeY;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            ClusterGridDefinition that = (ClusterGridDefinition) o;
            return Double.compare( that.clusterSizeX, clusterSizeX ) == 0 &&
                    Double.compare( that.clusterSizeY, clusterSizeY ) == 0 &&
                    Double.compare( that.extentMinX, extentMinX ) == 0 &&
                    Double.compare( that.extentMaxX, extentMaxX ) == 0 &&
                    Double.compare( that.extentMinY, extentMinY ) == 0 &&
                    Double.compare( that.extentMaxY, extentMaxY ) == 0 &&
                    Double.compare( that.extentX, extentX ) == 0 &&
                    Double.compare( that.extentY, extentY ) == 0 &&
                    pointCount == that.pointCount &&
                    clusterCountX == that.clusterCountX &&
                    clusterCountY == that.clusterCountY &&
                    pointsPerCluster == that.pointsPerCluster &&
                    Double.compare( that.clusterGapSizeX, clusterGapSizeX ) == 0 &&
                    Double.compare( that.clusterGapSizeY, clusterGapSizeY ) == 0 &&
                    Objects.equals( crsString, that.crsString ) &&
                    Objects.equals( clusterCornersFactory, that.clusterCornersFactory );
        }

        @Override
        public int hashCode()
        {
            return Objects
                    .hash( clusterSizeX, clusterSizeY, extentMinX, extentMaxX, extentMinY, extentMaxY, extentX, extentY,
                            pointCount, crsString, clusterCountX, clusterCountY, pointsPerCluster, clusterGapSizeX,
                            clusterGapSizeY,
                            clusterCornersFactory );
        }
    }

    private static class GridSpatialGeneratorFactory implements ValueGeneratorFactory<Point>
    {
        private double minX;
        private double minY;
        private long xStepCount;
        private long yStepCount;
        private long totalStepCount;
        private double xStep;
        private double yStep;
        private long count;
        private String crsString;

        private GridSpatialGeneratorFactory()
        {
        }

        private GridSpatialGeneratorFactory(
                double minX,
                double minY,
                long xStepCount,
                long yStepCount,
                double xStep,
                double yStep,
                CoordinateReferenceSystem crs )
        {
            this.minX = minX;
            this.minY = minY;
            this.xStepCount = xStepCount;
            this.yStepCount = yStepCount;
            this.totalStepCount = xStepCount * yStepCount;
            this.xStep = xStep;
            this.yStep = yStep;
            this.count = xStepCount * yStepCount;
            this.crsString = CRS.from( crs ).name();
        }

        @Override
        public ValueGeneratorFun<Point> create()
        {
            CoordinateReferenceSystem coordinateReferenceSystem = CRS.from( crsString ).crs();
            if ( coordinateReferenceSystem.isGeographic() )
            {
                // Cartesian is infinite in all directions, no restrictions required, but geographic has singularities at the poles,
                // hence the need for restrictions on Y.
                PointValue wrappedMinPoint = pointValue( coordinateReferenceSystem, minX, minY );
                return new GeographicPointValueGenerator( coordinateReferenceSystem, wrappedMinPoint, xStep, yStep, xStepCount, totalStepCount );
            }
            else
            {
                return new GeometricPointValueGenerator( coordinateReferenceSystem, minX, minY, xStep, yStep, xStepCount, totalStepCount );
            }
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            GridSpatialGeneratorFactory that = (GridSpatialGeneratorFactory) o;
            return Double.compare( that.minX, minX ) == 0 &&
                    Double.compare( that.minY, minY ) == 0 &&
                    xStepCount == that.xStepCount &&
                    yStepCount == that.yStepCount &&
                    totalStepCount == that.totalStepCount &&
                    Double.compare( that.xStep, xStep ) == 0 &&
                    Double.compare( that.yStep, yStep ) == 0 &&
                    count == that.count &&
                    Objects.equals( crsString, that.crsString );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( minX, minY, xStepCount, yStepCount, totalStepCount, xStep, yStep, count, crsString );
        }
    }

    private static class RandomSpatialGeneratorFactory implements ValueGeneratorFactory<Point>
    {
        private double minX;
        private double minY;
        private double maxX;
        private double maxY;
        private String crsString;

        private RandomSpatialGeneratorFactory()
        {
        }

        private RandomSpatialGeneratorFactory(
                double minX,
                double minY,
                double maxX,
                double maxY,
                CoordinateReferenceSystem crs )
        {
            this.minX = minX;
            this.minY = minY;
            this.maxX = maxX;
            this.maxY = maxY;
            this.crsString = CRS.from( crs ).name();
        }

        @Override
        public ValueGeneratorFun<Point> create()
        {
            return new ValueGeneratorFun<>()
            {
                private final CoordinateReferenceSystem crs = CRS.from( crsString ).crs();
                private final ValueGeneratorFun<Double> xs = randDouble( minX, maxX ).create();
                private final ValueGeneratorFun<Double> ys = randDouble( minY, maxY ).create();

                @Override
                public boolean wrapped()
                {
                    return false;
                }

                @Override
                public PointValue next( SplittableRandom rng )
                {
                    return pointValue( crs, xs.next( rng ), ys.next( rng ) );
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return next( rng );
                }
            };
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            RandomSpatialGeneratorFactory that = (RandomSpatialGeneratorFactory) o;
            return Double.compare( that.minX, minX ) == 0 &&
                    Double.compare( that.minY, minY ) == 0 &&
                    Double.compare( that.maxX, maxX ) == 0 &&
                    Double.compare( that.maxY, maxY ) == 0 &&
                    Objects.equals( crsString, that.crsString );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( minX, minY, maxX, maxY, crsString );
        }
    }

    private static class ClusteredSpatialGeneratorFactory implements ValueGeneratorFactory<Point>
    {
        private static final double GAUSSIAN_MEAN = 0D;
        private static final double GAUSSIAN_STD_DEV = 0.35D;
        private static final int GAUSSIAN_ATTEMPTS = 100;
        private ClusterGridDefinition definition;

        private ClusteredSpatialGeneratorFactory()
        {
        }

        private ClusteredSpatialGeneratorFactory( ClusterGridDefinition clusterGridDefinition )
        {
            this.definition = clusterGridDefinition;
        }

        @Override
        public ValueGeneratorFun<Point> create()
        {
            final double clusterDiameterX = definition.clusterSizeX / 2;
            final double clusterDiameterY = definition.clusterSizeY / 2;
            return new ValueGeneratorFun<>()
            {
                private final GaussianState gaussianState = new GaussianState();
                private final CoordinateReferenceSystem crs = definition.crs().crs();
                private final ValueGeneratorFun<Point> clusterCenters = definition.clusterCenters();
                private long currentCount;
                private PointValue clusterCenter;
                private long currentClusterX = -1;
                private long currentClusterY = -1;

                private boolean isClusterFull()
                {
                    return currentCount % definition.pointsPerCluster == 0;
                }

                private void moveToNextCluster()
                {
                    currentClusterX = (currentClusterX + 1) % definition.clusterCountX;
                    if ( 0 == currentClusterX )
                    {
                        currentClusterY = (currentClusterY + 1) % definition.clusterCountY;
                    }
                }

                @Override
                public boolean wrapped()
                {
                    return currentCount > 0 && currentCount % definition.pointCount == 0;
                }

                @Override
                public PointValue next( SplittableRandom rng )
                {
                    if ( isClusterFull() )
                    {
                        moveToNextCluster();
                        return nextClusterCenter( rng );
                    }
                    else
                    {
                        return nextClusterPoint( rng );
                    }
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return next( rng );
                }

                private PointValue nextClusterCenter( SplittableRandom rng )
                {
                    currentCount++;
                    return clusterCenter = (PointValue) clusterCenters.next( rng );
                }

                private PointValue nextClusterPoint( SplittableRandom rng )
                {
                    // Gaussian, limited to the range [-1,1]
                    double x = nextBoundedGaussian( rng );
                    double y = nextBoundedGaussian( rng );
                    // stretch Gaussian distribution to span the cluster size
                    double scaledX = x * clusterDiameterX;
                    double scaledY = y * clusterDiameterY;
                    // move Gaussian distribution to be centered over cluster center
                    double shiftedScaledX = scaledX + xFor( clusterCenter );
                    double shiftedScaledY = scaledY + yFor( clusterCenter );
                    currentCount++;
                    return pointValue( crs, shiftedScaledX, shiftedScaledY );
                }

                private double nextBoundedGaussian( SplittableRandom rng )
                {
                    for ( int i = 0; i < GAUSSIAN_ATTEMPTS; i++ )
                    {
                        double value = RichRandom.nextGaussian( gaussianState, rng ) * GAUSSIAN_STD_DEV;
                        if ( value < -1 || value > 1 )
                        {
                            continue;
                        }
                        return value + GAUSSIAN_MEAN;
                    }
                    throw new RuntimeException( "Took too long to find Gaussian value in range [-1,1]" );
                }
            };
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            ClusteredSpatialGeneratorFactory that = (ClusteredSpatialGeneratorFactory) o;
            return Objects.equals( definition, that.definition );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( definition );
        }
    }

    private static class CirclesSpatialGeneratorFactory implements ValueGeneratorFactory<Point>
    {
        private ClusterGridDefinition definition;

        private CirclesSpatialGeneratorFactory()
        {
        }

        private CirclesSpatialGeneratorFactory( ClusterGridDefinition clusterGridDefinition )
        {
            this.definition = clusterGridDefinition;
        }

        @Override
        public ValueGeneratorFun<Point> create()
        {
            if ( definition.clusterSizeX != definition.clusterSizeY )
            {
                throw new RuntimeException(
                        "X & Y cluster dimensions must be equal when generating circles\n" +
                                definition );
            }
            return new ValueGeneratorFun<>()
            {
                private final CoordinateReferenceSystem crs = definition.crs().crs();
                private final ValueGeneratorFun<Point> clusterCenters = definition.clusterCenters();
                final double clusterDiameter = definition.clusterSizeX / 2;
                private long currentCount;
                private Point clusterCenter;
                private long currentClusterX = -1;
                private long currentClusterY = -1;

                private boolean isClusterFull()
                {
                    return currentCount % definition.pointsPerCluster == 0;
                }

                private void moveToNextClusterX()
                {
                    currentClusterX = (currentClusterX + 1) % definition.clusterCountX;
                }

                private void moveToNextClusterY()
                {
                    currentClusterY = (currentClusterY + 1) % definition.clusterCountY;
                }

                @Override
                public boolean wrapped()
                {
                    return currentCount > 0 && currentCount % definition.pointCount == 0;
                }

                @Override
                public PointValue next( SplittableRandom rng )
                {
                    if ( isClusterFull() )
                    {
                        moveToNextClusterX();

                        if ( 0 == currentClusterX )
                        {
                            moveToNextClusterY();
                        }
                        clusterCenter = clusterCenters.next( rng );
                    }
                    currentCount++;
                    return nextClusterPoint( rng );
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return next( rng );
                }

                private PointValue nextClusterPoint( SplittableRandom rng )
                {
                    int xSign = (rng.nextDouble() > 0.5) ? -1 : 1;
                    int ySign = (rng.nextDouble() > 0.5) ? -1 : 1;
                    double theta = rng.nextDouble() * 90;
                    double x = clusterDiameter * sin( theta ) * xSign;
                    double y = clusterDiameter * cos( theta ) * ySign;
                    double shiftedX = x + xFor( clusterCenter );
                    double shiftedY = y + yFor( clusterCenter );
                    return pointValue( crs, shiftedX, shiftedY );
                }
            };
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            CirclesSpatialGeneratorFactory that = (CirclesSpatialGeneratorFactory) o;
            return Objects.equals( definition, that.definition );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( definition );
        }
    }

    static class DiagonalSpatialGeneratorFactory implements ValueGeneratorFactory<Point>
    {
        private ValueGeneratorFactory<Double> valueGeneratorFactory;
        private String crsString;

        private DiagonalSpatialGeneratorFactory()
        {
        }

        DiagonalSpatialGeneratorFactory( ValueGeneratorFactory<Double> valueGeneratorFactory, CoordinateReferenceSystem crs )
        {
            this.valueGeneratorFactory = valueGeneratorFactory;
            this.crsString = CRS.from( crs ).name();
        }

        @Override
        public ValueGeneratorFun<Point> create()
        {
            ValueGeneratorFun<Double> inner = valueGeneratorFactory.create();
            CoordinateReferenceSystem crs = CRS.from( crsString ).crs();
            return new ValueGeneratorFun<>()
            {
                @Override
                public boolean wrapped()
                {
                    return inner.wrapped();
                }

                @Override
                public PointValue next( SplittableRandom rng )
                {
                    double coordinate = inner.next( rng );
                    return pointValue( crs, coordinate, coordinate );
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return next( rng );
                }
            };
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            DiagonalSpatialGeneratorFactory that = (DiagonalSpatialGeneratorFactory) o;
            return Objects.equals( valueGeneratorFactory, that.valueGeneratorFactory ) &&
                    Objects.equals( crsString, that.crsString );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( valueGeneratorFactory, crsString );
        }
    }

    public static double xFor( Point point )
    {
        return point.getCoordinate().getCoordinate().get( 0 );
    }

    public static double yFor( Point point )
    {
        return point.getCoordinate().getCoordinate().get( 1 );
    }
}
