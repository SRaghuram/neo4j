# Copyright (c) 2002-2018 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.


<#
.SYNOPSIS
Sets a process-level environment variable value

.DESCRIPTION
Sets a process-level environment variable value.  This is a helper function which aids testing and mocking

.PARAMETER Name
Name of the environment variable

.PARAMETER Value
Value of the environment variable

.EXAMPLE
Set-Neo4jEnv 'Neo4jHome' 'C:\neo4j'

Sets the Neo4jHome environment variable to C:\neo4j

.OUTPUTS
System.String
Value of the environment variable

.NOTES
This function is private to the powershell module

#>
Function Set-Neo4jEnv
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low')]
  param (
    [Parameter(Mandatory=$true,ValueFromPipeline=$false,Position=0)]
    [String]$Name

    ,[Parameter(Mandatory=$true,ValueFromPipeline=$false,Position=1)]
    [String]$Value
  )

  Begin
  {
  }

  Process {
    Set-Item -Path Env:$Name -Value $Value
  }

  End
  {
  }
}
