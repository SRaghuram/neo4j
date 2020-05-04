/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.AdminAccessMode;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.IntSets;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AdminActionOnResource;
import org.neo4j.internal.kernel.api.security.LoginContext.IdLookup;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.Segment;

import static com.neo4j.server.security.enterprise.auth.Resource.Type.ALL_LABELS;
import static com.neo4j.server.security.enterprise.auth.Resource.Type.PROPERTY;
import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.DENY;
import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CONSTRAINT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DATABASE_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.INDEX;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.REMOVE_LABEL;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SET_LABEL;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TOKEN;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_PROPERTY_KEY;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

class StandardAccessMode implements AccessMode
{
    private final boolean allowsAccess;
    private final boolean allowsReads;
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

    private AdminAccessMode adminAccessMode;
    private AdminActionOnResource.DatabaseScope database;

    private StandardAccessMode(
            boolean allowsAccess,
            boolean allowsReads,
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

            AdminAccessMode adminAccessMode,
            String database
    )
    {
        this.allowsAccess = allowsAccess;
        this.allowsReads = allowsReads;
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
    public boolean allowsProcedureWith( String[] roleNames )
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

    static class Builder
    {
        private final boolean isAuthenticated;
        private final boolean passwordChangeRequired;
        private final Set<String> roles;
        private final IdLookup resolver;
        private final String database;
        private final String defaultDbName;

        private Map<ResourcePrivilege.GrantOrDeny,Boolean> anyAccess = new HashMap<>();  // track any access rights
        private Map<ResourcePrivilege.GrantOrDeny,Boolean> anyRead = new HashMap<>();  // track any reads for optimization purposes
        private Map<ResourcePrivilege.GrantOrDeny,Boolean> anyWrite = new HashMap<>(); // track any writes because we only support global write GRANT/DENY
        private boolean token;  // TODO - still to support GRANT/DENY
        private boolean schema; // TODO - still to support GRANT/DENY

        private Map<ResourcePrivilege.GrantOrDeny,Boolean> traverseAllLabels = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,Boolean> traverseAllRelTypes = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> traverseLabels = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> traverseRelTypes = new HashMap<>();

        private Map<ResourcePrivilege.GrantOrDeny,Boolean> setAllLabels = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> settableLabels = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,Boolean> removeAllLabels = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> removableLabels = new HashMap<>();

        private Map<ResourcePrivilege.GrantOrDeny,Boolean> createNodeWithAnyLabel = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> createNodeWithLabels = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,Boolean> deleteNodeWithAnyLabel = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> deleteNodeWithLabels = new HashMap<>();

        private Map<ResourcePrivilege.GrantOrDeny,Boolean> createRelationshipWithAnyType = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> createRelationshipWithTypes = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,Boolean> deleteRelationshipWithAnyType = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> deleteRelationshipWithTypes = new HashMap<>();

        private Map<ResourcePrivilege.GrantOrDeny,Boolean> readAllPropertiesAllLabels = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,Boolean> readAllPropertiesAllRelTypes = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> nodeSegmentForAllProperties = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> relationshipSegmentForAllProperties = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> nodeProperties = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> relationshipProperties = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,MutableIntObjectMap<IntSet>> nodeSegmentForProperty = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,MutableIntObjectMap<IntSet>> relationshipSegmentForProperty = new HashMap<>();

        private Map<ResourcePrivilege.GrantOrDeny,Boolean> writeAllPropertiesAllLabels = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,Boolean> writeAllPropertiesAllRelTypes = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> writeNodeSegmentForAllProperties = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> writeRelationshipSegmentForAllProperties = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> writeNodeProperties = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> writeRelationshipProperties = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,MutableIntObjectMap<IntSet>> writeNodeSegmentForProperty = new HashMap<>();
        private Map<ResourcePrivilege.GrantOrDeny,MutableIntObjectMap<IntSet>> writeRelationshipSegmentForProperty = new HashMap<>();

        private StandardAdminAccessMode.Builder adminModeBuilder = new StandardAdminAccessMode.Builder();

        Builder( boolean isAuthenticated, boolean passwordChangeRequired, Set<String> roles, IdLookup resolver, String database, String defaultDbName )
        {
            this.isAuthenticated = isAuthenticated;
            this.passwordChangeRequired = passwordChangeRequired;
            this.roles = roles;
            this.resolver = resolver;
            this.database = database;
            this.defaultDbName = defaultDbName;
            for ( ResourcePrivilege.GrantOrDeny privilegeType : ResourcePrivilege.GrantOrDeny.values() )
            {
                this.traverseLabels.put( privilegeType, IntSets.mutable.empty() );
                this.traverseRelTypes.put( privilegeType, IntSets.mutable.empty() );
                this.nodeSegmentForAllProperties.put( privilegeType, IntSets.mutable.empty() );
                this.relationshipSegmentForAllProperties.put( privilegeType, IntSets.mutable.empty() );
                this.nodeProperties.put( privilegeType, IntSets.mutable.empty() );
                this.relationshipProperties.put( privilegeType, IntSets.mutable.empty() );
                this.nodeSegmentForProperty.put( privilegeType, IntObjectMaps.mutable.empty() );
                this.relationshipSegmentForProperty.put( privilegeType, IntObjectMaps.mutable.empty() );
                this.settableLabels.put( privilegeType, IntSets.mutable.empty() );
                this.removableLabels.put( privilegeType, IntSets.mutable.empty() );
                this.createNodeWithLabels.put( privilegeType, IntSets.mutable.empty() );
                this.deleteNodeWithLabels.put( privilegeType, IntSets.mutable.empty() );
                this.createRelationshipWithTypes.put( privilegeType, IntSets.mutable.empty() );
                this.deleteRelationshipWithTypes.put( privilegeType, IntSets.mutable.empty() );
                this.writeNodeSegmentForAllProperties.put( privilegeType, IntSets.mutable.empty() );
                this.writeRelationshipSegmentForAllProperties.put( privilegeType, IntSets.mutable.empty() );
                this.writeNodeProperties.put( privilegeType, IntSets.mutable.empty() );
                this.writeRelationshipProperties.put( privilegeType, IntSets.mutable.empty() );
                this.writeNodeSegmentForProperty.put( privilegeType, IntObjectMaps.mutable.empty() );
                this.writeRelationshipSegmentForProperty.put( privilegeType, IntObjectMaps.mutable.empty() );
            }
        }

        StandardAccessMode build()
        {
            return new StandardAccessMode(
                    isAuthenticated && anyAccess.getOrDefault( GRANT, false ) && !anyAccess.getOrDefault( DENY, false ),
                    isAuthenticated && anyRead.getOrDefault( GRANT, false ),
                    isAuthenticated && anyWrite.getOrDefault( GRANT, false ),
                    anyWrite.getOrDefault( DENY, false ),
                    isAuthenticated && token,
                    isAuthenticated && schema,
                    passwordChangeRequired,
                    roles,

                    traverseAllLabels.getOrDefault( GRANT, false ),
                    traverseAllRelTypes.getOrDefault( GRANT, false ),
                    traverseLabels.get( GRANT ),
                    traverseRelTypes.get( GRANT ),

                    traverseAllLabels.getOrDefault( DENY, false ),
                    traverseAllRelTypes.getOrDefault( DENY, false ),
                    traverseLabels.get( DENY ),
                    traverseRelTypes.get( DENY ),

                    new PropertyPrivileges( readAllPropertiesAllLabels.getOrDefault( GRANT, false ),
                                            readAllPropertiesAllRelTypes.getOrDefault( GRANT, false ),
                                            nodeSegmentForAllProperties.get( GRANT ),
                                            relationshipSegmentForAllProperties.get( GRANT ),
                                            nodeProperties.get( GRANT ),
                                            relationshipProperties.get( GRANT ),
                                            nodeSegmentForProperty.get( GRANT ),
                                            relationshipSegmentForProperty.get( GRANT ) ),

                    new PropertyPrivileges( readAllPropertiesAllLabels.getOrDefault( DENY, false ),
                                            readAllPropertiesAllRelTypes.getOrDefault( DENY, false ),
                                            nodeSegmentForAllProperties.get( DENY ),
                                            relationshipSegmentForAllProperties.get( DENY ),
                                            nodeProperties.get( DENY ),
                                            relationshipProperties.get( DENY ),
                                            nodeSegmentForProperty.get( DENY ),
                                            relationshipSegmentForProperty.get( DENY ) ),

                    setAllLabels.getOrDefault( GRANT, false ),
                    settableLabels.get( GRANT ),
                    setAllLabels.getOrDefault( DENY, false ),
                    settableLabels.get( DENY ),
                    removeAllLabels.getOrDefault( GRANT, false ),
                    removableLabels.get( GRANT ),
                    removeAllLabels.getOrDefault( DENY, false ),
                    removableLabels.get( DENY ),

                    createNodeWithAnyLabel.getOrDefault( GRANT, false ),
                    createNodeWithLabels.get( GRANT ),
                    createNodeWithAnyLabel.getOrDefault( DENY, false ),
                    createNodeWithLabels.get( DENY ),
                    deleteNodeWithAnyLabel.getOrDefault( GRANT, false ),
                    deleteNodeWithLabels.get( GRANT ),
                    deleteNodeWithAnyLabel.getOrDefault( DENY, false ),
                    deleteNodeWithLabels.get( DENY ),

                    createRelationshipWithAnyType.getOrDefault( GRANT, false ),
                    createRelationshipWithTypes.get( GRANT ),
                    createRelationshipWithAnyType.getOrDefault( DENY, false ),
                    createRelationshipWithTypes.get( DENY ),
                    deleteRelationshipWithAnyType.getOrDefault( GRANT, false ),
                    deleteRelationshipWithTypes.get( GRANT ),
                    deleteRelationshipWithAnyType.getOrDefault( DENY, false ),
                    deleteRelationshipWithTypes.get( DENY ),

                    new PropertyPrivileges( writeAllPropertiesAllLabels.getOrDefault( GRANT, false ),
                                            writeAllPropertiesAllRelTypes.getOrDefault( GRANT, false ),
                                            writeNodeSegmentForAllProperties.get( GRANT ),
                                            writeRelationshipSegmentForAllProperties.get( GRANT ),
                                            writeNodeProperties.get( GRANT ),
                                            writeRelationshipProperties.get( GRANT ),
                                            writeNodeSegmentForProperty.get( GRANT ),
                                            writeRelationshipSegmentForProperty.get( GRANT ) ),

                    new PropertyPrivileges( writeAllPropertiesAllLabels.getOrDefault( DENY, false ),
                                            writeAllPropertiesAllRelTypes.getOrDefault( DENY, false ),
                                            writeNodeSegmentForAllProperties.get( DENY ),
                                            writeRelationshipSegmentForAllProperties.get( DENY ),
                                            writeNodeProperties.get( DENY ),
                                            writeRelationshipProperties.get( DENY ),
                                            writeNodeSegmentForProperty.get( DENY ),
                                            writeRelationshipSegmentForProperty.get( DENY ) ),

                    adminModeBuilder.build(),
                    database );
        }

        Builder withAccess()
        {
            anyAccess.put( GRANT, true );
            return this;
        }

        Builder addPrivilege( ResourcePrivilege privilege )
        {
            Resource resource = privilege.getResource();
            Segment segment = privilege.getSegment();
            ResourcePrivilege.GrantOrDeny privilegeType = privilege.getPrivilegeType();
            PrivilegeAction action = privilege.getAction();

            switch ( action )
            {
            case ACCESS:
                anyAccess.put( privilegeType, true );
                break;

            case TRAVERSE:
                anyRead.put( privilegeType, true );
                handleAllResourceQualifier( segment, privilegeType, "traverse", traverseAllLabels, traverseLabels, traverseAllRelTypes, traverseRelTypes );
                break;

            case READ:
                anyRead.put( privilegeType, true );
                handleReadPrivilege( resource, segment, privilegeType, "read" );
                break;

            case MATCH:
                anyRead.put( privilegeType, true );
                if ( !(privilegeType.isDeny() && resource.type() == PROPERTY) )
                {
                    // don't deny TRAVERSE for DENY MATCH {prop}
                    handleAllResourceQualifier( segment, privilegeType, "match", traverseAllLabels, traverseLabels, traverseAllRelTypes, traverseRelTypes );
                }
                handleReadPrivilege( resource, segment, privilegeType, "match" );
                break;

            case WRITE:
                anyWrite.put( privilegeType, true );
                break;

            case SET_LABEL:
            case REMOVE_LABEL:
                handleLabelPrivilege( resource, privilegeType, action );
                break;

            case CREATE_ELEMENT:
                handleCreatePrivilege( segment, privilegeType );
                break;

            case DELETE_ELEMENT:
                handleDeletePrivilege( segment, privilegeType );
                break;
            case SET_PROPERTY:
                handleSetPropertyPrivilege( resource, segment, privilegeType );
                break;
            default:
                if ( TOKEN.satisfies( action ) )
                {
                    token = true;
                    addPrivilegeAction( privilege );
                }
                else if ( INDEX.satisfies( action ) || CONSTRAINT.satisfies( action ) )
                {
                    schema = true;
                    addPrivilegeAction( privilege );
                }
                else if ( ADMIN.satisfies( action ) )
                {
                    addPrivilegeAction( privilege );
                }
                else if ( DATABASE_ACTIONS.satisfies( action ) )
                {
                    anyAccess.put( privilegeType, true );
                    token = true;
                    schema = true;
                    addPrivilegeAction( privilege );
                }
            }
            return this;
        }

        private void handleCreatePrivilege( Segment segment, ResourcePrivilege.GrantOrDeny privilegeType )
        {
            if ( segment instanceof LabelSegment )
            {
                if ( segment.equals( LabelSegment.ALL ) )
                {
                    createNodeWithAnyLabel.put( privilegeType, true );
                }
                else
                {
                    addLabel( createNodeWithLabels.get( privilegeType ), (LabelSegment) segment );
                }
            }
            else if ( segment instanceof RelTypeSegment )
            {
                if ( segment.equals( RelTypeSegment.ALL ) )
                {
                    createRelationshipWithAnyType.put( privilegeType, true );
                }
                else
                {
                    addRelType( createRelationshipWithTypes.get( privilegeType ), (RelTypeSegment) segment );
                }
            }
            else
            {
                throw new IllegalStateException( "Unsupported segment qualifier for create privilege: " + segment.getClass().getSimpleName() );
            }
        }

        private void handleDeletePrivilege( Segment segment, ResourcePrivilege.GrantOrDeny privilegeType )
        {
            if ( segment instanceof LabelSegment )
            {
                if ( segment.equals( LabelSegment.ALL ) )
                {
                    deleteNodeWithAnyLabel.put( privilegeType, true );
                }
                else
                {
                    addLabel( deleteNodeWithLabels.get( privilegeType ), (LabelSegment) segment );
                }
            }
            else if ( segment instanceof RelTypeSegment )
            {
                if ( segment.equals( RelTypeSegment.ALL ) )
                {
                    deleteRelationshipWithAnyType.put( privilegeType, true );
                }
                else
                {
                    addRelType( deleteRelationshipWithTypes.get( privilegeType ), (RelTypeSegment) segment );
                }
            }
            else
            {
                throw new IllegalStateException( "Unsupported segment qualifier for delete privilege: " + segment.getClass().getSimpleName() );
            }
        }

        private void handleLabelPrivilege( Resource resource, ResourcePrivilege.GrantOrDeny privilegeType, PrivilegeAction action )
        {
            if ( resource.type().equals( ALL_LABELS ) )
            {
                if ( action == SET_LABEL )
                {
                    setAllLabels.put( privilegeType, true );
                }
                else if ( action == REMOVE_LABEL )
                {
                    removeAllLabels.put( privilegeType, true );
                }
            }
            else
            {
                if ( action == SET_LABEL )
                {
                    addLabelResource( settableLabels.get( privilegeType ), (Resource.LabelResource) resource );
                }
                else if ( action == REMOVE_LABEL )
                {
                    addLabelResource( removableLabels.get( privilegeType ), (Resource.LabelResource) resource );
                }
            }
        }

        private void handleSetPropertyPrivilege( Resource resource, Segment segment, ResourcePrivilege.GrantOrDeny privilegeType )
        {
            switch ( resource.type() )
            {
            case PROPERTY:
                handlePropertyResource( resource, segment, privilegeType, "set property", writeNodeProperties, writeNodeSegmentForProperty,
                                        writeRelationshipProperties, writeRelationshipSegmentForProperty );
                break;
            case ALL_PROPERTIES:
                handleAllResourceQualifier( segment, privilegeType, "set property", writeAllPropertiesAllLabels, writeNodeSegmentForAllProperties,
                                            writeAllPropertiesAllRelTypes, writeRelationshipSegmentForAllProperties );
                break;
            default:
            }
        }

        private void handleReadPrivilege( Resource resource, Segment segment, ResourcePrivilege.GrantOrDeny privilegeType, String privilegeName )
        {
            switch ( resource.type() )
            {
            case GRAPH:
                readAllPropertiesAllLabels.put( privilegeType, true );
                readAllPropertiesAllRelTypes.put( privilegeType, true );
                break;
            case PROPERTY:
                handlePropertyResource( resource, segment, privilegeType, privilegeName, nodeProperties, nodeSegmentForProperty, relationshipProperties,
                                        relationshipSegmentForProperty );
                break;
            case ALL_PROPERTIES:
                handleAllResourceQualifier( segment, privilegeType, privilegeName, readAllPropertiesAllLabels, nodeSegmentForAllProperties,
                                            readAllPropertiesAllRelTypes, relationshipSegmentForAllProperties );
                break;
            default:
            }
        }

        private void handlePropertyResource( Resource propertyResource, Segment segment, ResourcePrivilege.GrantOrDeny privilegeType, String privilegeName,
                                             Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> nodeProperties,
                                             Map<ResourcePrivilege.GrantOrDeny,MutableIntObjectMap<IntSet>> nodeSegmentForProperty,
                                             Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> relationshipProperties,
                                             Map<ResourcePrivilege.GrantOrDeny,MutableIntObjectMap<IntSet>> relationshipSegmentForProperty )
        {
            int propertyId = resolvePropertyId( propertyResource.getArg1() );
            if ( propertyId == ANY_PROPERTY_KEY )
            {
                return;
            }
            if ( segment instanceof LabelSegment )
            {
                if ( segment.equals( LabelSegment.ALL ) )
                {
                    nodeProperties.get( privilegeType ).add( propertyId );
                }
                else
                {
                    addLabel( nodeSegmentForProperty.get( privilegeType ), (LabelSegment) segment, propertyId );
                }
            }
            else if ( segment instanceof RelTypeSegment )
            {
                if ( segment.equals( RelTypeSegment.ALL ) )
                {
                    relationshipProperties.get( privilegeType ).add( propertyId );
                }
                else
                {
                    addRelType( relationshipSegmentForProperty.get( privilegeType ), (RelTypeSegment) segment, propertyId );
                }
            }
            else
            {
                throw new IllegalStateException(
                        "Unsupported segment qualifier for " + privilegeName + " privilege: " + segment.getClass().getSimpleName() );
            }
        }

        private void handleAllResourceQualifier( Segment segment, ResourcePrivilege.GrantOrDeny privilegeType, String privilegeName,
                                                 Map<ResourcePrivilege.GrantOrDeny,Boolean> allPropertiesAllLabels,
                                                 Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> nodeSegmentForAllProperties,
                                                 Map<ResourcePrivilege.GrantOrDeny,Boolean> allPropertiesAllRelTypes,
                                                 Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> relationshipSegmentForAllProperties )
        {
            if ( segment instanceof LabelSegment )
            {
                if ( segment.equals( LabelSegment.ALL ) )
                {
                    allPropertiesAllLabels.put( privilegeType, true );
                }
                else
                {
                    addLabel( nodeSegmentForAllProperties.get( privilegeType ), (LabelSegment) segment );
                }
            }
            else if ( segment instanceof RelTypeSegment )

            {
                if ( segment.equals( RelTypeSegment.ALL ) )
                {
                    allPropertiesAllRelTypes.put( privilegeType, true );
                }
                else
                {
                    addRelType( relationshipSegmentForAllProperties.get( privilegeType ), (RelTypeSegment) segment );
                }
            }
            else
            {
                throw new IllegalStateException(
                        "Unsupported segment qualifier for " + privilegeName + " privilege: " + segment.getClass().getSimpleName() );
            }
        }

        private void addPrivilegeAction( ResourcePrivilege privilege )
        {
            AdminActionOnResource.DatabaseScope dbScope;
            if ( privilege.appliesToAll() )
            {
                dbScope = AdminActionOnResource.DatabaseScope.ALL;
            }
            else if ( privilege.appliesToDefault() )
            {
                dbScope = new AdminActionOnResource.DatabaseScope( defaultDbName );
            }
            else
            {
                dbScope = new AdminActionOnResource.DatabaseScope( privilege.getDbName() );
            }
            var adminAction = new AdminActionOnResource( privilege.getAction(), dbScope, privilege.getSegment() );
            if ( privilege.getPrivilegeType().isGrant() )
            {
                adminModeBuilder.allow( adminAction );
            }
            else
            {
                adminModeBuilder.deny( adminAction );
            }
        }

        private int resolveLabelId( String label )
        {
            assert !label.isEmpty();
            return resolver.getLabelId( label );
        }

        private int resolveRelTypeId( String relType )
        {
            assert !relType.isEmpty();
            return resolver.getRelTypeId( relType );
        }

        private int resolvePropertyId( String property )
        {
            assert !property.isEmpty();
            return resolver.getPropertyKeyId( property );
        }

        private void addLabel( MutableIntObjectMap<IntSet> map, LabelSegment segment, int propertyId )
        {
            // propertyId has already been checked that it is not ANY_LABEL
            MutableIntSet setForProperty = (MutableIntSet) map.getIfAbsentPut( propertyId, IntSets.mutable.empty() );
            addLabel( setForProperty, segment );
        }

        private void addLabel( MutableIntSet whiteOrBlacklist, LabelSegment segment )
        {
            int labelId = resolveLabelId( segment.getLabel() );
            if ( labelId != ANY_LABEL )
            {
                whiteOrBlacklist.add( labelId );
            }
        }

        private void addRelType( MutableIntObjectMap<IntSet> map, RelTypeSegment segment, int propertyId )
        {
            // propertyId has already been checked that it is not ANY_RELATIONSHIP_TYPE
            MutableIntSet setForProperty = (MutableIntSet) map.getIfAbsentPut( propertyId, IntSets.mutable.empty() );
            addRelType( setForProperty, segment );
        }

        private void addRelType( MutableIntSet whiteOrBlacklist, RelTypeSegment segment )
        {
            int relTypeId = resolveRelTypeId( segment.getRelType() );
            if ( relTypeId != ANY_RELATIONSHIP_TYPE )
            {
                whiteOrBlacklist.add( relTypeId );
            }
        }

        private void addLabelResource( MutableIntSet whiteOrBlacklist, Resource.LabelResource resource )
        {
            int labelId = resolveLabelId( resource.getArg1() );
            if ( labelId != ANY_LABEL )
            {
                whiteOrBlacklist.add( labelId );
            }
        }
    }
}
