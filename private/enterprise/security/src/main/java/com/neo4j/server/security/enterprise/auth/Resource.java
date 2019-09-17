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
            if ( !(action.equals( Action.WRITE ) || action.equals( Action.READ ) || action.equals( Action.TRAVERSE )) )
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

    class DatabaseResource implements Resource
    {
        @Override
        public void assertValidCombination( Action action ) throws InvalidArgumentsException
        {
            if ( !(action.equals( Action.ACCESS ) || action.equals( Action.START ) || action.equals( Action.STOP )) )
            {
                throw new InvalidArgumentsException( String.format( "Database resource cannot be combined with action '%s'", action.toString() ) );
            }
        }

        @Override
        public Type type()
        {
            return Type.DATABASE;
        }

        @Override
        public String toString()
        {
            return "database";
        }

        @Override
        public int hashCode()
        {
            return Type.DATABASE.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj instanceof DatabaseResource;
        }
    }

    class AllPropertiesResource implements Resource
    {
        public AllPropertiesResource( )
        {
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
        public Type type()
        {
            return Type.ALL_PROPERTIES;
        }

        @Override
        public String toString()
        {
            return "all properties ";
        }

        @Override
        public int hashCode()
        {
            return Type.ALL_PROPERTIES.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj == this || obj instanceof AllPropertiesResource;
        }
    }

    class PropertyResource implements Resource
    {
        private final String property;

        public PropertyResource( String property )
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
            return property == null ? 0 : property.hashCode() + 31 * Type.PROPERTY.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj == this )
            {
                return true;
            }

            if ( obj instanceof PropertyResource )
            {
                PropertyResource other = (PropertyResource) obj;
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
            int hash = nameSpace == null ? 0 : nameSpace.hashCode();
            return hash + 31 * (procedure == null ? 0 : procedure.hashCode());
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj == this )
            {
                return true;
            }

            if ( obj instanceof ProcedureResource )
            {
                ProcedureResource other = (ProcedureResource) obj;
                boolean equals = other.nameSpace == null && this.nameSpace == null || other.nameSpace != null && other.nameSpace.equals( this.nameSpace );
                return equals && (other.procedure == null && this.procedure == null || other.procedure != null && other.procedure.equals( this.procedure ));
            }
            return false;
        }
    }

    enum Type
    {
        ALL_PROPERTIES,
        PROPERTY,
        GRAPH,
        DATABASE,
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
