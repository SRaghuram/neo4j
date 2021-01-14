/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.data;

import org.neo4j.values.storable.CoordinateReferenceSystem;

import static java.lang.String.format;

public abstract class CRS
{
    public static CRS from( String crsString )
    {
        switch ( crsString )
        {
        case Cartesian.NAME:
            return new Cartesian();
        case WGS84.NAME:
            return new WGS84();
        default:
            throw new IllegalArgumentException( format( "Invalid coordinate reference system: '%s'", crsString ) );
        }
    }

    public static CRS from( CoordinateReferenceSystem crs )
    {
        if ( CoordinateReferenceSystem.Cartesian.equals( crs ) )
        {
            return new Cartesian();
        }
        else if ( CoordinateReferenceSystem.WGS84.equals( crs ) )
        {
            return new WGS84();
        }
        else
        {
            throw new IllegalArgumentException( format( "Invalid coordinate reference system: '%s'", crs ) );
        }
    }

    public abstract CoordinateReferenceSystem crs();

    public abstract String name();

    public static class Cartesian extends CRS
    {
        public static final String NAME = "cartesian";

        @Override
        public CoordinateReferenceSystem crs()
        {
            return CoordinateReferenceSystem.Cartesian;
        }

        @Override
        public String name()
        {
            return NAME;
        }

        @Override
        public int hashCode()
        {
            return getClass().hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            return getClass().equals( obj.getClass() );
        }
    }

    public static class WGS84 extends CRS
    {
        public static final String NAME = "wgs84";

        @Override
        public CoordinateReferenceSystem crs()
        {
            return CoordinateReferenceSystem.WGS84;
        }

        @Override
        public String name()
        {
            return NAME;
        }

        @Override
        public int hashCode()
        {
            return getClass().hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            return getClass().equals( obj.getClass() );
        }
    }
}
