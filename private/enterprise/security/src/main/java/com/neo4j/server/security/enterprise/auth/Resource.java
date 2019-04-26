/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.Action;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

public interface Resource
{
    static Resource parse( String type, String arg1, String arg2 ) throws InvalidArgumentsException
    {
        try
        {
            Resource.Type resource = Type.valueOf( type.toUpperCase() );
            switch ( resource )
            {
            case GRAPH:
                return new GraphResource();
            case LABEL:
                return new LabelResource( arg1, arg2 );
            case TOKEN:
                return new TokenResource();
            case SCHEMA:
                return new SchemaResource();
            case SYSTEM:
                return new SystemResource();
            case PROCEDURE:
                return new ProcedureResource( arg1, arg2 );
            default:
                throw new InvalidArgumentsException( String.format( "'%s' is not a supported resource", type ) );
            }
        }
        catch ( IllegalArgumentException e )
        {
            throw new InvalidArgumentsException( String.format( "'%s' is not a valid resource", type ) );
        }
    }

    void assertValidCombination( Action action ) throws InvalidArgumentsException;

    default String getArg1()
    {
        return "";
    }

    default String getArg2()
    {
        return "";
    }

    Type type();

    class GraphResource implements Resource
    {
        @Override
        public void assertValidCombination( Action action ) throws InvalidArgumentsException
        {
            if ( !(action.equals( Action.WRITE ) || action.equals( Action.READ )) )
            {
                throw new InvalidArgumentsException( String.format( "Graph resource cannot be combined with action '%s'", action.toString() ) );
            }
        }

        @Override
        public Type type()
        {
            return Type.GRAPH;
        }

        @Override
        public String toString()
        {
            return "graph";
        }

        @Override
        public int hashCode()
        {
            return Type.GRAPH.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj instanceof GraphResource;
        }
    }

    class LabelResource implements Resource
    {
        private final String label;
        private final String property;

        public LabelResource( String label, String property )
        {
            this.label = label;
            this.property = property;
        }

        @Override
        public void assertValidCombination( Action action ) throws InvalidArgumentsException
        {
            if ( !(action.equals( Action.WRITE ) || action.equals( Action.READ )) )
            {
                throw new InvalidArgumentsException( String.format( "Label resource cannot be combined with action '%s'", action.toString() ) );
            }
        }

        @Override
        public String getArg1()
        {
            return label == null ? "" : label;
        }

        @Override
        public String getArg2()
        {
            return property == null ? "" : property;
        }

        @Override
        public Type type()
        {
            return Type.LABEL;
        }

        @Override
        public String toString()
        {
            return "label " + label;
        }

        @Override
        public int hashCode()
        {
            return label == null ? 0 : label.hashCode() + (property == null ? 0 : 7 * property.hashCode()) + 13 * Type.GRAPH.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj == this )
            {
                return true;
            }

            if ( obj instanceof LabelResource )
            {
                LabelResource other = (LabelResource) obj;
                return (this.label == null && other.label == null || this.label != null && this.label.equals( other.label )) &&
                       (this.property == null && other.property == null || (this.property != null && this.property.equals( other.property )));
            }
            return false;
        }
    }

    class PropertyResource implements Resource
    {
        private final String property;

        PropertyResource( String property )
        {
            this.property = property;
        }

        @Override
        public void assertValidCombination( Action action ) throws InvalidArgumentsException
        {
            if ( !(action.equals( Action.WRITE ) || action.equals( Action.READ )) )
            {
                throw new InvalidArgumentsException( String.format( "Property resource cannot be combined with action `%s`", action.toString() ) );
            }
        }

        @Override
        public String getArg1()
        {
            return property == null ? "" : property;
        }

        @Override
        public Type type()
        {
            return Type.PROPERTY;
        }

        @Override
        public String toString()
        {
            return "property " + property;
        }

        @Override
        public int hashCode()
        {
            return property == null ? 0 : 7 * property.hashCode() + 13 * Type.GRAPH.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj == this )
            {
                return true;
            }

            if ( obj instanceof LabelResource )
            {
                LabelResource other = (LabelResource) obj;
                return this.property == null && other.property == null || this.property != null && this.property.equals( other.property );
            }
            return false;
        }
    }

    class TokenResource implements Resource
    {
        @Override
        public void assertValidCombination( Action action ) throws InvalidArgumentsException
        {
            if ( !action.equals( Action.WRITE ) )
            {
                throw new InvalidArgumentsException( String.format( "Token resource cannot be combined with action '%s'", action.toString() ) );
            }
        }

        @Override
        public Type type()
        {
            return Type.TOKEN;
        }

        @Override
        public String toString()
        {
            return "token";
        }

        @Override
        public int hashCode()
        {
            return Type.TOKEN.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj instanceof TokenResource;
        }
    }

    class SchemaResource implements Resource
    {
        @Override
        public void assertValidCombination( Action action ) throws InvalidArgumentsException
        {
            if ( !action.equals( Action.WRITE ) )
            {
                throw new InvalidArgumentsException( String.format( "Schema resource cannot be combined with action '%s'", action.toString() ) );
            }
        }

        @Override
        public Type type()
        {
            return Type.SCHEMA;
        }

        @Override
        public String toString()
        {
            return "schema";
        }

        @Override
        public int hashCode()
        {
            return Type.SCHEMA.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj instanceof SchemaResource;
        }
    }

    class SystemResource implements Resource
    {
        @Override
        public void assertValidCombination( Action action ) throws InvalidArgumentsException
        {
            if ( !action.equals( Action.WRITE ) )
            {
                throw new InvalidArgumentsException( String.format( "System resource cannot be combined with action '%s'", action.toString() ) );
            }
        }

        @Override
        public Type type()
        {
            return Type.SYSTEM;
        }

        @Override
        public String toString()
        {
            return "system";
        }

        @Override
        public int hashCode()
        {
            return Type.SYSTEM.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj instanceof SystemResource;
        }
    }

    class ProcedureResource implements Resource
    {
        private final String nameSpace;
        private final String procedure;

        public ProcedureResource( String nameSpace, String procedure )
        {
            this.nameSpace = nameSpace;
            this.procedure = procedure;
        }

        @Override
        public void assertValidCombination( Action action ) throws InvalidArgumentsException
        {
            if ( !action.equals( Action.EXECUTE ) )
            {
                throw new InvalidArgumentsException( String.format( "Procedure resource cannot be combined with action '%s'", action.toString() ) );
            }
        }

        @Override
        public String getArg1()
        {
            return nameSpace == null ? "" : nameSpace;
        }

        @Override
        public String getArg2()
        {
            return procedure == null ? "" : procedure;
        }

        @Override
        public Type type()
        {
            return Type.PROCEDURE;
        }

        @Override
        public String toString()
        {
            return "library " + nameSpace + " procedure " + procedure;
        }

        @Override
        public int hashCode()
        {
            return Type.PROCEDURE.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj instanceof ProcedureResource;
        }
    }

    enum Type
    {
        PROPERTY,
        LABEL,
        GRAPH,
        TOKEN,
        SCHEMA,
        SYSTEM,
        PROCEDURE;

        @Override
        public String toString()
        {
            return super.toString().toLowerCase();
        }
    }
}
