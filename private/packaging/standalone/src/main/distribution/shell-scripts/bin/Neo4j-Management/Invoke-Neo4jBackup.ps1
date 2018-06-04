# Copyright (c) 2002-2018 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.


<#
.SYNOPSIS
Invokes Neo4j Backup utility

.DESCRIPTION
Invokes Neo4j Backup utility

.PARAMETER CommandArgs
The remaining command line arguments to pass to the Neo4j Backup

.OUTPUTS
System.Int32
0 = Success
non-zero = an error occured

.NOTES
Only supported on version 3.x Neo4j Enterprise Edition databases

#>
Function Invoke-Neo4jBackup
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low')]
  param (
    [parameter(Mandatory=$false,ValueFromRemainingArguments=$true)]
    [object[]]$CommandArgs = @()
  )
  
  Begin
  {
  }
  
  Process
  {
    try {
      Return [int](Invoke-Neo4jUtility -Command 'Backup' -CommandArgs $CommandArgs -ErrorAction 'Stop')      
    }
    catch {
      Write-Error $_
      Return 1
    }
  }
  
  End
  {
  }
}
