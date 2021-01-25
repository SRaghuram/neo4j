/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.tool.macro;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.neo4j.bench.common.util.BenchmarkUtil;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import static java.lang.String.format;

@JsonTypeInfo( use = JsonTypeInfo.Id.NAME )
@JsonSubTypes( {@JsonSubTypes.Type( Deployment.Server.class ),
                @JsonSubTypes.Type( Deployment.Embedded.class )} )
public abstract class Deployment implements DeploymentMode
{
    protected static DeploymentModes mode;

    @JsonCreator
    public Deployment( @JsonProperty( "mode" ) DeploymentModes mode )
    {
        this.mode = mode;
    }

    public static Deployment parse( String value )
    {
        if ( value.toUpperCase().equals( DeploymentModes.EMBEDDED.name() ) )
        {
            return new Embedded();
        }
        else if ( value.toUpperCase().startsWith( Server.VALUE_PREFIX ) )
        {
            String neo4jDirString = value.substring( Server.VALUE_PREFIX.length() );
            return server( neo4jDirString );
        }
        else
        {
            throw new RuntimeException( format( "Invalid deployment mode value: '%s'", value ) );
        }
    }

    public DeploymentModes deploymentModes()
    {
        return mode;
    }

    @Override
    public String name()
    {
        return mode.name();
    }

    public static Deployment server( String neo4jDir )
    {
        Objects.requireNonNull( neo4jDir, "Neo4j server directory cannot be null" );
        return new Server( neo4jDir );
    }

    public abstract void assertExists();

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

    @JsonTypeName( "embedded" )
    public static class Embedded extends Deployment
    {

        @JsonCreator
        public Embedded()
        {
            super( DeploymentModes.EMBEDDED );
        }

        @Override
        public String parsableValue()
        {
            return mode.name();
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

        @Override
        public void assertExists()
        {
            //We can always run EMBEDDED we do not need any other files then the NAME, so we do not have to check that it exists.
        }
    }

    @JsonTypeName( "server" )
    public static class Server extends Deployment
    {
        private static final String VALUE_PREFIX = DeploymentModes.SERVER.name() + ":";

        private final String path;

        @JsonCreator
        public Server( @JsonProperty( "path" ) String path )
        {
            super( DeploymentModes.SERVER );
            this.path = path;
        }

        /**
         * WARNING: Never call this explicitly.
         * No-params constructor is only used for JSON (de)serialization.
         */
        public Server()
        {
            this( null );
        }

        public Path path()
        {
            return Paths.get( path );
        }

        @Override
        public String parsableValue()
        {
            return VALUE_PREFIX + path;
        }

        @Override
        public void assertExists()
        {
            BenchmarkUtil.assertDirectoryExists( path() );
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
