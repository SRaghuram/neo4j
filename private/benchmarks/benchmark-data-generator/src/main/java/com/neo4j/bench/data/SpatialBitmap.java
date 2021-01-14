/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.data;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.file.Path;

import javax.imageio.ImageIO;

import static java.lang.Math.min;
import static java.lang.Math.round;

public abstract class SpatialBitmap
{
    public static SpatialBitmap createFor(
            BufferedImage image,
            int xExtentMin,
            int xExtentMax,
            int yExtentMin,
            int yExtentMax )
    {
        return new PrettySpatialBitmap( image, xExtentMin, xExtentMax, yExtentMin, yExtentMax );
    }

    public static SpatialBitmap none()
    {
        return new NothingSpatialBitmap();
    }

    public abstract void addPointToBitmap( double x, double y );

    public abstract void writeTo( Path path );

    private static class PrettySpatialBitmap extends SpatialBitmap
    {
        private final BufferedImage image;
        private final int xOffset;
        private final int yOffset;
        private final int xExtent;
        private final int yExtent;
        private final int xPixelCount;
        private final int yPixelCount;
        private final int[][] counts;

        PrettySpatialBitmap(
                BufferedImage image,
                int xExtentMin,
                int xExtentMax,
                int yExtentMin,
                int yExtentMax )
        {
            this.image = image;
            this.xOffset = xExtentMin;
            this.yOffset = yExtentMin;
            this.xExtent = xExtentMax - xExtentMin;
            this.yExtent = yExtentMax - yExtentMin;
            this.xPixelCount = image.getWidth();
            this.yPixelCount = image.getHeight();
            this.counts = new int[xPixelCount][yPixelCount];
        }

        @Override
        public void addPointToBitmap( double x, double y )
        {
            // move coordinate back to minimum of zero
            x = x - xOffset;
            y = y - yOffset;
            // scale coordinates to size of image
            int xPixel = (int) round( x / xExtent * xPixelCount );
            int yPixel = (int) round( y / yExtent * yPixelCount );
            // prevents out of bounds due to above calculation round up at the upper bounds
            xPixel = min( xPixel, xPixelCount - 1 );
            yPixel = min( yPixel, yPixelCount - 1 );
            counts[xPixel][yPixel] += 1;
        }

        @Override
        public void writeTo( Path path )
        {
            int maxCount = Integer.MIN_VALUE;
            for ( int x = 0; x < xPixelCount; x++ )
            {
                for ( int y = 0; y < yPixelCount; y++ )
                {
                    maxCount = Math.max( maxCount, counts[x][y] );
                }
            }
            for ( int x = 0; x < xPixelCount; x++ )
            {
                for ( int y = 0; y < yPixelCount; y++ )
                {
                    int notRed = (255 * counts[x][y]) / maxCount;
                    image.setRGB( x, y, new Color( 255, 255 - notRed, 255 - notRed ).getRGB() );
                }
            }

            try
            {
                ImageIO.write( image, "PNG", path.toFile() );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Error writing .png file", e );
            }
        }
    }

    private static class NothingSpatialBitmap extends SpatialBitmap
    {
        @Override
        public void addPointToBitmap( double x, double y )
        {
            // do nothing
        }

        @Override
        public void writeTo( Path path )
        {
            // do nothing
        }
    }
}
