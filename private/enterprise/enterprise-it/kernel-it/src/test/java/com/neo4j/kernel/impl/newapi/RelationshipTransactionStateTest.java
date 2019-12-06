package com.neo4j.kernel.impl.newapi;

import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.newapi.RelationshipTransactionStateTestBase;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RelationshipTransactionStateTest extends RelationshipTransactionStateTestBase<EnterpriseWriteTestSupport>
{
    @Override
    public EnterpriseWriteTestSupport newTestSupport()
    {
        return new EnterpriseWriteTestSupport();
    }

    @Test
    void shouldCountNewRelationshipsRestrictedUser() throws Exception
    {
        int relationship;
        try ( KernelTransaction tx = beginTransaction() )
        {
            Write write = tx.dataWrite();
            relationship = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            write.relationshipCreate( write.nodeCreate(), relationship, write.nodeCreate() );
            tx.commit();
        }

        Kernel kernel = testSupport.kernelToTest();
        try ( KernelTransaction tx = kernel.beginTransaction( KernelTransaction.Type.implicit, loginContext() ) )
        {
            Write write = tx.dataWrite();
            write.relationshipCreate( write.nodeCreate(), relationship, write.nodeCreate() );

            long countsTxState = tx.dataRead().countsForRelationship( -1, relationship, -1 );
            long countsNoTxState = tx.dataRead().countsForRelationshipWithoutTxState( -1, relationship, -1 );

            assertEquals( 2, countsTxState );
            assertEquals( 1, countsNoTxState );
        }
    }

    private LoginContext loginContext()
    {
        return new EnterpriseSecurityContext( AuthSubject.AUTH_DISABLED, new FakeAccessMode(), Collections.emptySet(), action -> false );
    }

    // This access mode pretends to have explicit permission to traverse relationships and nodes and have write privileges.
    static class FakeAccessMode implements AccessMode
    {
        @Override
        public boolean allowsWrites()
        {
            return true;
        }

        @Override
        public boolean allowsTokenCreates( PrivilegeAction action )
        {
            return true;
        }

        @Override
        public boolean allowsSchemaWrites()
        {
            return false;
        }

        @Override
        public boolean allowsSchemaWrites( PrivilegeAction action )
        {
            return false;
        }

        @Override
        public boolean allowsTraverseAllLabels()
        {
            return false;
        }

        @Override
        public boolean allowsTraverseAllNodesWithLabel( long label )
        {
            return false;
        }

        @Override
        public boolean allowsSeeLabelToken( long label )
        {
            return true;
        }

        @Override
        public boolean disallowsTraverseLabel( long label )
        {
            return false;
        }

        @Override
        public boolean allowsTraverseNode( long... labels )
        {
            return true;
        }

        @Override
        public boolean allowsTraverseAllRelTypes()
        {
            return false;
        }

        @Override
        public boolean allowsTraverseRelType( int relType )
        {
            return true;
        }

        @Override
        public boolean allowsReadPropertyAllLabels( int propertyKey )
        {
            return false;
        }

        @Override
        public boolean disallowsReadPropertyForSomeLabel( int propertyKey )
        {
            return true;
        }

        @Override
        public boolean allowsReadNodeProperty( Supplier<LabelSet> labels, int propertyKey )
        {
            return false;
        }

        @Override
        public boolean allowsReadPropertyAllRelTypes( int propertyKey )
        {
            return false;
        }

        @Override
        public boolean allowsReadRelationshipProperty( IntSupplier relType, int propertyKey )
        {
            return false;
        }

        @Override
        public boolean allowsSeePropertyKeyToken( int propertyKey )
        {
            return false;
        }

        @Override
        public boolean allowsProcedureWith( String[] allowed )
        {
            return false;
        }

        @Override
        public AuthorizationViolationException onViolation( String msg )
        {
            return null;
        }

        @Override
        public String name()
        {
            return null;
        }
    }
}
