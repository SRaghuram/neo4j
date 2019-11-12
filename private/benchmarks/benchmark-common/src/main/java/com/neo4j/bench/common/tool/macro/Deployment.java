/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.tool.macro;

import com.neo4j.bench.common.util.BenchmarkUtil;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.nio.file.Path;
import java.nio.file.Paths;

import static java.lang.String.format;

public abstract class Deployment implements DeploymentMode
{
    public static Deployment parse( String value )
    {
        if ( value.toUpperCase().equals( Embedded.NAME ) )
        {
            return new Embedded();
        }
        else if ( value.toUpperCase().startsWith( Server.VALUE_PREFIX ) )
        {
            String neo4jDirString = value.substring( Server.VALUE_PREFIX.length() );
            Path neo4jDir = Paths.get( neo4jDirString );
            return server( neo4jDir );
        }
        else
        {
            throw new RuntimeException( format( "Invalid deployment mode value: '%s'", value ) );
        }
    }

    public static Deployment server( Path neo4jDir )
    {
        BenchmarkUtil.assertDirectoryExists( neo4jDir );
        return new Server( neo4jDir );
    }

    public static Deployment embedded()
    {
        return new Embedded();
    }

    public static DeploymentMode server()
    {
        return new Server( null );
    }

    @Override
    public String toString()
    {
        return parsableValue();
    }

    public static class Embedded extends Deployment
    {
        private static final String NAME = "EMBEDDED";

        @Override
        public String name()
        {
            return NAME;
        }

        @Override
        public String parsableValue()
        {
            return NAME;
        }

        @Override
        public int hashCode()
        {
            return new HashCodeBuilder().append( name() ).toHashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj == null )
            {
                return false;
            }
            if ( obj == this )
            {
                return true;
            }
            if ( obj.getClass() != getClass() )
            {
                return false;
            }
            Embedded that = (Embedded) obj;
            return new EqualsBuilder().append( name(), that.name() ).isEquals();
        }
    }

    public static class Server extends Deployment
    {
        private static final String NAME = "SERVER";
        private static final String VALUE_PREFIX = NAME + ":";

        private final Path path;

        private Server( Path path )
        {
            this.path = path;
        }

        public Path path()
        {
            return path;
        }

        @Override
        public String name()
        {
            return NAME;
        }

        @Override
        public String parsableValue()
        {
            return VALUE_PREFIX + path.toAbsolutePath();
        }

        @Override
        public int hashCode()
        {
            return new HashCodeBuilder()
                    .append( name() )
                    .append( path )
                    .toHashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj == null )
            {
                return false;
            }
            if ( obj == this )
            {
                return true;
            }
            if ( obj.getClass() != getClass() )
            {
                return false;
            }
            Server that = (Server) obj;
            return new EqualsBuilder()
                    .append( name(), that.name() )
                    .append( path(), that.path() )
                    .isEquals();
        }
    }
}
