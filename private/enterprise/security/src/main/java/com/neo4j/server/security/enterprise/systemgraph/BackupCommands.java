package com.neo4j.server.security.enterprise.systemgraph;

import java.util.List;

public class BackupCommands
{
    public final List<String> roleSetup;
    public final List<String> userSetup;

    public BackupCommands( List<String> roleSetup, List<String> userSetup ) {
        this.roleSetup = roleSetup;
        this.userSetup = userSetup;
    }
}
