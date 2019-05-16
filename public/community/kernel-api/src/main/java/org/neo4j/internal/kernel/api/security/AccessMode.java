/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.kernel.api.security;

import java.util.function.Supplier;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.kernel.api.exceptions.Status;

/** Controls the capabilities of a KernelTransaction. */
public interface AccessMode
{
    enum Static implements AccessMode
    {
        /** No reading or writing allowed. */
        NONE( false, false, false, false, false ),
        /** No reading or writing allowed because of expired credentials. */
        CREDENTIALS_EXPIRED( false, false, false, false, false )
                {
                    @Override
                    public AuthorizationViolationException onViolation( String msg )
                    {
                        return new AuthorizationViolationException( String.format(
                                msg + "%n%nThe credentials you provided were valid, but must be " +
                                "changed before you can " +
                                "use this instance. If this is the first time you are using Neo4j, this is to " +
                                "ensure you are not using the default credentials in production. If you are not " +
                                "using default credentials, you are getting this message because an administrator " +
                                "requires a password change.%n" +
                                "Changing your password is easy to do via the Neo4j Browser.%n" +
                                "If you are connecting via a shell or programmatically via a driver, " +
                                "just issue a `CALL dbms.changePassword('new password')` statement in the current " +
                                "session, and then restart your driver with the new password configured." ),
                                Status.Security.CredentialsExpired );
                    }
                },

        /** Allows reading data and schema, but not writing. */
        READ( true, false, false, false, false ),
        /** Allows writing data */
        WRITE_ONLY( false, true, false, false, false ),
        /** Allows reading and writing data, but not schema. */
        WRITE( true, true, false, false, false ),
        /** Allows reading and writing data and creating new tokens, but not schema. */
        TOKEN_WRITE( true, true, true, false, false ),
        /** Allows all operations. */
        FULL( true, true, true, true, true );

        private final boolean read;
        private final boolean write;
        private final boolean token;
        private final boolean schema;
        private final boolean procedure;

        Static( boolean read, boolean write, boolean token, boolean schema, boolean procedure )
        {
            this.read = read;
            this.write = write;
            this.token = token;
            this.schema = schema;
            this.procedure = procedure;
        }

        @Override
        public boolean allowsTokenReads()
        {
            return read || write || token || schema;
        }

        @Override
        public boolean allowsReads()
        {
            return read;
        }

        @Override
        public boolean allowsWrites()
        {
            return write;
        }

        @Override
        public boolean allowsTokenCreates()
        {
            return token;
        }

        @Override
        public boolean allowsSchemaWrites()
        {
            return schema;
        }

        @Override
        public boolean allowsTraverseAllLabels()
        {
            return read;
        }

        @Override
        public boolean allowsTraverseLabels( int... labels )
        {
            return read;
        }

        @Override
        public boolean allowsReadPropertyAllLabels( int propertyKey )
        {
            return read;
        }

        @Override
        public boolean allowsReadProperty( Supplier<LabelSet> labels, int propertyKey )
        {
            return read;
        }

        @Override
        public boolean allowsPropertyReads( int propertyKey )
        {
            return read;
        }

        @Override
        public boolean allowsProcedureWith( String[] allowed )
        {
            return procedure;
        }

        @Override
        public AuthorizationViolationException onViolation( String msg )
        {
            return new AuthorizationViolationException( msg );
        }
    }

    boolean allowsTokenReads();
    boolean allowsReads();
    boolean allowsWrites();
    boolean allowsTokenCreates();
    boolean allowsSchemaWrites();

    boolean allowsTraverseAllLabels();
    boolean allowsTraverseLabels( int... labels );
    default boolean allowsTraverseLabels( long... labels )
    {
        int[] asInt = new int[labels.length];
        for ( int i = 0; i < labels.length; i++ )
        {
            asInt[i] = (int) labels[i];
        }
        return allowsTraverseLabels( asInt );
    }

    boolean allowsReadPropertyAllLabels( int propertyKey );
    boolean allowsReadProperty( Supplier<LabelSet> labels, int propertyKey );

    boolean allowsPropertyReads( int propertyKey );

    /**
     * Determines whether this mode allows execution of a procedure with the parameter string array in its
     * procedure annotation.
     *
     * @param allowed An array of strings that encodes permissions that allows the execution of a procedure
     * @return {@code true} if this mode allows the execution of a procedure with the given parameter string array
     * encoding permission
     */
    boolean allowsProcedureWith( String[] allowed );

    AuthorizationViolationException onViolation( String msg );
    String name();

    default boolean isOverridden()
    {
        return false;
    }
}
