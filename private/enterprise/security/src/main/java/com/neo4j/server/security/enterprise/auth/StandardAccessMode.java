/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.AdminAccessMode;
import org.eclipse.collections.api.set.primitive.IntSet;

import java.util.Set;
import java.util.TreeSet;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AdminActionOnResource;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.Segment;

import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

class StandardAccessMode implements AccessMode
{
    private final boolean allowsAccess;
    private final boolean allowsWrites;
    private final boolean disallowWrites;
    private final boolean allowsTokenCreates;
    private final boolean allowsSchemaWrites;
    private final boolean passwordChangeRequired;
    private final Set<String> roles;

    private final boolean allowsTraverseAllLabels;
    private final boolean allowsTraverseAllRelTypes;
    private final IntSet whitelistTraverseLabels;
    private final IntSet whitelistTraverseRelTypes;

    private final boolean disallowsTraverseAllLabels;
    private final boolean disallowsTraverseAllRelTypes;
    private final IntSet blacklistTraverseLabels;
    private final IntSet blacklistTraverseRelTypes;

    private final IntSet whitelistedNodePropertiesForSomeLabel;
    private final IntSet whitelistedRelationshipPropertiesForSomeType;

    private final PropertyPrivileges readAllow;
    private final PropertyPrivileges readDisallow;

    private final boolean allowsSetAllLabels;
    private final IntSet whitelistSetLabels;
    private final boolean disallowSetAllLabels;
    private final IntSet blacklistSetLabels;
    private final boolean allowsRemoveAllLabels;
    private final IntSet whitelistRemoveLabels;
    private final boolean disallowRemoveAllLabels;
    private final IntSet blacklistRemoveLabels;

    private final boolean allowsCreateNodeAllLabels;
    private final IntSet whitelistCreateNodeWithLabels;
    private final boolean disallowCreateNodeAllLabels;
    private final IntSet blacklistCreateNodeWithLabels;
    private final boolean allowsDeleteNodeAllLabels;
    private final IntSet whitelistDeleteNodeWithLabels;
    private final boolean disallowDeleteNodeAllLabels;
    private final IntSet blacklistDeleteNodeWithLabels;

    private final boolean allowsCreateRelationshipAllTypes;
    private final IntSet whitelistCreateRelationshipWithTypes;
    private final boolean disallowCreateRelationshipAllTypes;
    private final IntSet blacklistCreateRelationshipWithTypes;
    private final boolean allowsDeleteRelationshipAllTypes;
    private final IntSet whitelistDeleteRelationshipWithTypes;
    private final boolean disallowDeleteRelationshipAllTypes;
    private final IntSet blacklistDeleteRelationshipWithTypes;

    private final PropertyPrivileges writeAllow;
    private final PropertyPrivileges writeDisallow;

    private ProcedurePrivileges procedurePrivileges;
    private AdminAccessMode adminAccessMode;
    private AdminActionOnResource.DatabaseScope database;

    StandardAccessMode(
            boolean allowsAccess,
            boolean allowsWrites,
            boolean disallowWrites,
            boolean allowsTokenCreates,
            boolean allowsSchemaWrites,
            boolean passwordChangeRequired,
            Set<String> roles,

            boolean allowsTraverseAllLabels,
            boolean allowsTraverseAllRelTypes,
            IntSet whitelistTraverseLabels,
            IntSet whitelistTraverseRelTypes,

            boolean disallowsTraverseAllLabels,
            boolean disallowsTraverseAllRelTypes,
            IntSet blacklistTraverseLabels,
            IntSet blacklistTraverseRelTypes,

            PropertyPrivileges readAllow,
            PropertyPrivileges readDisallow,

            boolean allowsSetAllLabels,
            IntSet whitelistSetLabels,
            boolean disallowSetAllLabels,
            IntSet blacklistSetLabels,
            boolean allowsRemoveAllLabels,
            IntSet whitelistRemoveLabels,
            boolean disallowRemoveAllLabels,
            IntSet blacklistRemoveLabels,

            boolean allowsCreateNodeAllLabels,
            IntSet whitelistCreateNodeWithLabels,
            boolean disallowCreateNodeAllLabels,
            IntSet blacklistCreateNodeWithLabels,
            boolean allowsDeleteNodeAllLabels,
            IntSet whitelistDeleteNodeWithLabels,
            boolean disallowDeleteNodeAllLabels,
            IntSet blacklistDeleteNodeWithLabels,

            boolean allowsCreateRelationshipAllTypes,
            IntSet whitelistCreateRelationshipWithTypes,
            boolean disallowCreateRelationshipAllTypes,
            IntSet blacklistCreateRelationshipWithTypes,
            boolean allowsDeleteRelationshipAllTypes,
            IntSet whitelistDeleteRelationshipWithTypes,
            boolean disallowDeleteRelationshipAllTypes,
            IntSet blacklistDeleteRelationshipWithTypes,

            PropertyPrivileges writeAllow,
            PropertyPrivileges writeDisallow,

            ProcedurePrivileges procedurePrivileges,

            AdminAccessMode adminAccessMode,
            String database
    )
    {
        this.allowsAccess = allowsAccess;
        this.allowsWrites = allowsWrites;
        this.disallowWrites = disallowWrites;
        this.allowsTokenCreates = allowsTokenCreates;
        this.allowsSchemaWrites = allowsSchemaWrites;
        this.passwordChangeRequired = passwordChangeRequired;
        this.roles = roles;

        this.allowsTraverseAllLabels = allowsTraverseAllLabels;
        this.allowsTraverseAllRelTypes = allowsTraverseAllRelTypes;
        this.whitelistTraverseLabels = whitelistTraverseLabels;
        this.whitelistTraverseRelTypes = whitelistTraverseRelTypes;

        this.disallowsTraverseAllLabels = disallowsTraverseAllLabels;
        this.disallowsTraverseAllRelTypes = disallowsTraverseAllRelTypes;
        this.blacklistTraverseLabels = blacklistTraverseLabels;
        this.blacklistTraverseRelTypes = blacklistTraverseRelTypes;

        this.readAllow = readAllow;
        this.readDisallow = readDisallow;

        this.allowsSetAllLabels = allowsSetAllLabels;
        this.whitelistSetLabels = whitelistSetLabels;
        this.disallowSetAllLabels = disallowSetAllLabels;
        this.blacklistSetLabels = blacklistSetLabels;
        this.allowsRemoveAllLabels = allowsRemoveAllLabels;
        this.whitelistRemoveLabels = whitelistRemoveLabels;
        this.disallowRemoveAllLabels = disallowRemoveAllLabels;
        this.blacklistRemoveLabels = blacklistRemoveLabels;

        this.allowsCreateNodeAllLabels = allowsCreateNodeAllLabels;
        this.whitelistCreateNodeWithLabels = whitelistCreateNodeWithLabels;
        this.disallowCreateNodeAllLabels = disallowCreateNodeAllLabels;
        this.blacklistCreateNodeWithLabels = blacklistCreateNodeWithLabels;
        this.allowsDeleteNodeAllLabels = allowsDeleteNodeAllLabels;
        this.whitelistDeleteNodeWithLabels = whitelistDeleteNodeWithLabels;
        this.disallowDeleteNodeAllLabels = disallowDeleteNodeAllLabels;
        this.blacklistDeleteNodeWithLabels = blacklistDeleteNodeWithLabels;

        this.allowsCreateRelationshipAllTypes = allowsCreateRelationshipAllTypes;
        this.whitelistCreateRelationshipWithTypes = whitelistCreateRelationshipWithTypes;
        this.disallowCreateRelationshipAllTypes = disallowCreateRelationshipAllTypes;
        this.blacklistCreateRelationshipWithTypes = blacklistCreateRelationshipWithTypes;
        this.allowsDeleteRelationshipAllTypes = allowsDeleteRelationshipAllTypes;
        this.whitelistDeleteRelationshipWithTypes = whitelistDeleteRelationshipWithTypes;
        this.disallowDeleteRelationshipAllTypes = disallowDeleteRelationshipAllTypes;
        this.blacklistDeleteRelationshipWithTypes = blacklistDeleteRelationshipWithTypes;
        this.writeAllow = writeAllow;
        this.writeDisallow = writeDisallow;

        this.procedurePrivileges = procedurePrivileges;

        this.adminAccessMode = adminAccessMode;
        this.database = new AdminActionOnResource.DatabaseScope( database );

        this.whitelistedNodePropertiesForSomeLabel =
                readAllow.getLabelsForProperty().keySet()
                         .select( key -> !readAllow.getLabelsForProperty().get( key ).isEmpty() );

        this.whitelistedRelationshipPropertiesForSomeType =
                readAllow.getRelTypesForProperty().keySet()
                         .select( key -> !readAllow.getRelTypesForProperty().get( key ).isEmpty() );
    }

    enum PermissionState
    {
        NOT_GRANTED,
        EXPLICIT_GRANT,
        EXPLICIT_DENY;

        public PermissionState combine( PermissionState p )
        {
            int order = this.compareTo( p );
            if ( order <= 0 )
            {
                return p;
            }
            else
            {
                return this;
            }
        }

        public boolean allowsAccess()
        {
            return this == EXPLICIT_GRANT;
        }

        public static PermissionState fromWhitelist( boolean permitted )
        {
            if ( permitted )
            {
                return EXPLICIT_GRANT;
            }
            else
            {
                return NOT_GRANTED;
            }
        }

        public static PermissionState fromBlacklist( boolean permitted )
        {
            if ( permitted )
            {
                return NOT_GRANTED;
            }
            else
            {
                return EXPLICIT_DENY;
            }
        }
    }

    @Override
    public boolean allowsWrites()
    {
        return allowsWrites && !disallowWrites;
    }

    @Override
    public boolean allowsTokenCreates( PrivilegeAction action )
    {
        return allowsTokenCreates && adminAccessMode.allows( new AdminActionOnResource( action, database, Segment.ALL ) );
    }

    @Override
    public boolean allowsSchemaWrites()
    {
        return allowsSchemaWrites;
    }

    @Override
    public boolean allowsSchemaWrites( PrivilegeAction action )
    {
        return allowsSchemaWrites && adminAccessMode.allows( new AdminActionOnResource( action, database, Segment.ALL ) );
    }

    @Override
    public boolean allowsTraverseAllLabels()
    {
        return allowsTraverseAllLabels && !disallowsTraverseAllLabels && blacklistTraverseLabels.isEmpty();
    }

    @Override
    public boolean allowsTraverseAllNodesWithLabel( long label )
    {
        // Note: we do not check blacklistTraverseLabels.contains(label) because this should be a first check
        // to be followed by the explicit blacklist check in disallowsTraverseLabel
        if ( disallowsTraverseAllLabels || blacklistTraverseLabels.notEmpty() )
        {
            return false;
        }
        return allowsTraverseAllLabels || whitelistTraverseLabels.contains( (int) label );
    }

    @Override
    public boolean disallowsTraverseLabel( long label )
    {
        return disallowsTraverseAllLabels || blacklistTraverseLabels.contains( (int) label );
    }

    @Override
    public boolean allowsTraverseNode( long... labels )
    {
        if ( allowsTraverseAllLabels() )
        {
            return true;
        }
        if ( disallowsTraverseAllLabels || labels.length == 1 && labels[0] == ANY_LABEL )
        {
            return false;
        }

        boolean allowedTraverseAnyLabel = false;

        for ( long labelAsLong : labels )
        {
            int label = (int) labelAsLong;
            if ( blacklistTraverseLabels.contains( label ) )
            {
                return false;
            }
            else if ( whitelistTraverseLabels.contains( label ) )
            {
                allowedTraverseAnyLabel = true;
            }
        }

        return allowedTraverseAnyLabel || allowsTraverseAllLabels;
    }

    @Override
    public boolean allowsTraverseAllRelTypes()
    {
        return allowsTraverseAllRelTypes && !disallowsTraverseAllRelTypes && blacklistTraverseRelTypes.isEmpty();
    }

    @Override
    public boolean allowsTraverseRelType( int relType )
    {
        if ( allowsTraverseAllRelTypes() )
        {
            return true;
        }
        if ( relType == ANY_RELATIONSHIP_TYPE || disallowsTraverseAllRelTypes || blacklistTraverseRelTypes.contains( relType ) )
        {
            return false;
        }

        return allowsTraverseAllRelTypes || whitelistTraverseRelTypes.contains( relType );
    }

    @Override
    public boolean allowsReadPropertyAllLabels( int propertyKey )
    {
        return (readAllow.isAllPropertiesAllLabels() || readAllow.getNodePropertiesForAllLabels().contains( propertyKey )) &&
               !disallowsReadPropertyForSomeLabel( propertyKey );
    }

    @Override
    public boolean disallowsReadPropertyForSomeLabel( int propertyKey )
    {
        return disallowsPropertyForSomeLabel( propertyKey, readDisallow );
    }

    private boolean disallowsPropertyForSomeLabel( int propertyKey, PropertyPrivileges disallow )
    {
        return disallow.isAllPropertiesAllLabels() || disallow.getNodePropertiesForAllLabels().contains( propertyKey ) ||
               !disallow.getLabelsForAllProperties().isEmpty() ||
               disallow.getLabelsForProperty().get( propertyKey ) != null;
    }

    private boolean disallowsReadPropertyForAllLabels( int propertyKey )
    {
        return readDisallow.isAllPropertiesAllLabels() || readDisallow.getNodePropertiesForAllLabels().contains( propertyKey );
    }

    @Override
    public boolean allowsReadNodeProperty( Supplier<TokenSet> labelSupplier, int propertyKey )
    {
        if ( allowsReadPropertyAllLabels( propertyKey ) )
        {
            return true;
        }
        if ( disallowsReadPropertyForAllLabels( propertyKey ) )
        {
            return false;
        }
        return canAccessNodeProperty( labelSupplier, propertyKey, readAllow, readDisallow ).allowsAccess();
    }

    private PermissionState canAccessNodeProperty( Supplier<TokenSet> labelSupplier, int propertyKey, PropertyPrivileges allow, PropertyPrivileges disallow )
    {

        TokenSet tokenSet = labelSupplier.get();
        IntSet whiteListed = allow.getLabelsForProperty().get( propertyKey );
        IntSet blackListed = disallow.getLabelsForProperty().get( propertyKey );

        boolean allowed = false;

        for ( long labelAsLong : tokenSet.all() )
        {
            int label = (int) labelAsLong;
            if ( whiteListed != null && whiteListed.contains( label ) )
            {
                allowed = true;
            }
            if ( blackListed != null && blackListed.contains( label ) )
            {
                return PermissionState.EXPLICIT_DENY;
            }
            if ( allow.getLabelsForAllProperties().contains( label ) )
            {
                allowed = true;
            }
            if ( disallow.getLabelsForAllProperties().contains( label ) )
            {
                return PermissionState.EXPLICIT_DENY;
            }
        }
        return PermissionState.fromWhitelist( allowed || allow.isAllPropertiesAllLabels() || allow.getNodePropertiesForAllLabels().contains( propertyKey ) );
    }

    @Override
    public boolean allowsReadPropertyAllRelTypes( int propertyKey )
    {
        return (readAllow.isAllPropertiesAllRelTypes() || readAllow.getRelationshipPropertiesForAllTypes().contains( propertyKey )) &&
               !disallowsPropertyForSomeRelType( propertyKey, readDisallow );
    }

    private boolean disallowsPropertyForSomeRelType( int propertyKey, PropertyPrivileges disallow )
    {
        return disallow.isAllPropertiesAllRelTypes() || disallow.getRelationshipPropertiesForAllTypes().contains( propertyKey ) ||
               !disallow.getRelTypesForAllProperties().isEmpty() || disallow.getRelTypesForProperty().get( propertyKey ) != null;
    }

    private boolean disallowsReadPropertyForAllRelTypes( int propertyKey )
    {
        return readDisallow.isAllPropertiesAllRelTypes() || readDisallow.getRelationshipPropertiesForAllTypes().contains( propertyKey );
    }

    @Override
    public boolean allowsReadRelationshipProperty( IntSupplier relType, int propertyKey )
    {
        if ( allowsReadPropertyAllRelTypes( propertyKey ) )
        {
            return true;
        }

        if ( disallowsReadPropertyForAllRelTypes( propertyKey ) )
        {
            return false;
        }
        return canAccessRelProperty( relType, propertyKey, readAllow, readDisallow).allowsAccess();
    }

    private PermissionState canAccessRelProperty( IntSupplier relType, int propertyKey, PropertyPrivileges allow, PropertyPrivileges disallow )
    {
        IntSet whitelisted = allow.getRelTypesForProperty().get( propertyKey );
        IntSet blacklisted = disallow.getRelTypesForProperty().get( propertyKey );

        boolean allowed =
                (whitelisted != null && whitelisted.contains( relType.getAsInt() )) ||
                allow.getRelTypesForAllProperties().contains( relType.getAsInt() ) ||
                allow.isAllPropertiesAllRelTypes() || allow.getRelationshipPropertiesForAllTypes().contains( propertyKey );

        boolean disallowedRelType =
                (blacklisted != null && blacklisted.contains( relType.getAsInt() )) ||
                disallow.getRelTypesForAllProperties().contains( relType.getAsInt() );

        return PermissionState.fromBlacklist( !disallowedRelType ).combine( PermissionState.fromWhitelist( allowed ));
    }

    @Override
    public boolean allowsSeePropertyKeyToken( int propertyKey )
    {
        boolean disabledForNodes =
                readDisallow.isAllPropertiesAllLabels() || readDisallow.getNodePropertiesForAllLabels().contains( propertyKey ) ||
                !allowPropertyReadOnSomeNode( propertyKey );

        boolean disabledForRels = readDisallow.isAllPropertiesAllRelTypes() || readDisallow.getRelationshipPropertiesForAllTypes().contains( propertyKey ) ||
                                  !allowsPropertyReadOnSomeRelType( propertyKey );

        return !(disabledForNodes && disabledForRels);
    }

    private boolean allowPropertyReadOnSomeNode( int propertyKey )
    {
        return readAllow.isAllPropertiesAllLabels() || readAllow.getNodePropertiesForAllLabels().contains( propertyKey ) ||
               whitelistedNodePropertiesForSomeLabel.contains( propertyKey ) || readAllow.getLabelsForAllProperties().notEmpty();
    }

    private boolean allowsPropertyReadOnSomeRelType( int propertyKey )
    {
        return readAllow.isAllPropertiesAllRelTypes() || readAllow.getRelationshipPropertiesForAllTypes().contains( propertyKey ) ||
               whitelistedRelationshipPropertiesForSomeType.contains( propertyKey ) || readAllow.getRelTypesForAllProperties().notEmpty();
    }

    @Override
    public boolean shouldBoostAccessForProcedureWith( String[] roleNames )
    {
        for ( String roleName : roleNames )
        {
            if ( roles.contains( roleName ) )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean allowsExecuteProcedure( int procedureId )
    {
        return procedurePrivileges.allowsExecuteProcedure( procedureId ) ;
    }

    @Override
    public boolean allowsSetLabel( long labelId )
    {
        if ( disallowWrites || disallowSetAllLabels || blacklistSetLabels.contains( (int) labelId ) )
        {
            return false;
        }
        return allowsWrites || allowsSetAllLabels || whitelistSetLabels.contains( (int) labelId );
    }

    @Override
    public boolean allowsRemoveLabel( long labelId )
    {
        if ( disallowWrites || disallowRemoveAllLabels || blacklistRemoveLabels.contains( (int) labelId ) )
        {
            return false;
        }
        return allowsWrites || allowsRemoveAllLabels || whitelistRemoveLabels.contains( (int) labelId );
    }

    @Override
    public boolean allowsCreateNode( int[] labelIds )
    {
        if ( disallowWrites || disallowCreateNodeAllLabels )
        {
            return false;
        }

        boolean allowed = false;
        if ( labelIds != null && labelIds.length > 0 )
        {
            allowed = true;
            for ( int label : labelIds )
            {
                if ( blacklistCreateNodeWithLabels.contains( label ) )
                {
                    return false;
                }
                allowed &= whitelistCreateNodeWithLabels.contains( label );
            }
        }

        return allowsWrites || allowsCreateNodeAllLabels || allowed;
    }

    @Override
    public boolean allowsDeleteNode( Supplier<TokenSet> labelSupplier )
    {
        if ( disallowWrites || disallowDeleteNodeAllLabels )
        {
            return false;
        }

        boolean allowed = false;
        long[] labelIds = labelSupplier.get().all();

        if ( labelIds.length > 0 )
        {
            allowed = true;
            for ( long label : labelIds )
            {
                if ( blacklistDeleteNodeWithLabels.contains( (int) label ) )
                {
                    return false;
                }
                allowed &= whitelistDeleteNodeWithLabels.contains( (int) label );
            }
        }

        return allowsWrites || allowsDeleteNodeAllLabels || allowed;
    }

    @Override
    public boolean allowsCreateRelationship( int relType )
    {
        if ( disallowWrites || disallowCreateRelationshipAllTypes || blacklistCreateRelationshipWithTypes.contains( relType ) )
        {
            return false;
        }
        return allowsWrites || allowsCreateRelationshipAllTypes || whitelistCreateRelationshipWithTypes.contains( relType );
    }

    @Override
    public boolean allowsDeleteRelationship( int relType )
    {
        if ( disallowWrites || disallowDeleteRelationshipAllTypes || blacklistDeleteRelationshipWithTypes.contains( relType ) )
        {
            return false;
        }
        return allowsWrites || allowsDeleteRelationshipAllTypes || whitelistDeleteRelationshipWithTypes.contains( relType );
    }

    @Override
    public boolean allowsSetProperty( Supplier<TokenSet> labelIds, int propertyKey )
    {
        if ( disallowWrites )
        {
            return false;
        }
        if ( (writeAllow.isAllPropertiesAllLabels() || writeAllow.getNodePropertiesForAllLabels().contains( propertyKey )) &&
             !disallowsPropertyForSomeLabel( propertyKey, writeDisallow ) )
        {
            return true;
        }
        if ( writeDisallow.isAllPropertiesAllLabels() || writeDisallow.getNodePropertiesForAllLabels().contains( propertyKey ) )
        {
            return false;
        }
        return canAccessNodeProperty( labelIds, propertyKey, writeAllow, writeDisallow )
                .combine( PermissionState.fromWhitelist( allowsWrites ) ).allowsAccess();
    }

    @Override
    public boolean allowsSetProperty( IntSupplier relType, int propertyKey )
    {
        if ( disallowWrites )
        {
            return false;
        }
        if ( (writeDisallow.isAllPropertiesAllRelTypes() || writeDisallow.getRelationshipPropertiesForAllTypes().contains( propertyKey )) &&
             !disallowsPropertyForSomeRelType( propertyKey, writeDisallow ) )
        {
            return true;
        }
        if ( writeDisallow.isAllPropertiesAllRelTypes() || writeDisallow.getRelationshipPropertiesForAllTypes().contains( propertyKey ) )
        {
            return false;
        }
        return canAccessRelProperty( relType, propertyKey, writeAllow, writeDisallow )
                .combine( PermissionState.fromWhitelist( allowsWrites ) ).allowsAccess();
    }

    @Override
    public AuthorizationViolationException onViolation( String msg )
    {
        if ( passwordChangeRequired )
        {
            return Static.CREDENTIALS_EXPIRED.onViolation( "Permission denied." );
        }
        else
        {
            return new AuthorizationViolationException( msg );
        }
    }

    @Override
    public String name()
    {
        Set<String> sortedRoles = new TreeSet<>( roles );
        return roles.isEmpty() ? "no roles" : "roles " + sortedRoles.toString();
    }

    boolean allowsAccess()
    {
        return allowsAccess;
    }

    @Override
    public Set<String> roles()
    {
        return roles;
    }

    AdminAccessMode getAdminAccessMode()
    {
        return adminAccessMode;
    }
}
