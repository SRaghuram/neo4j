SSH onto the Vildvittra machine that needs setting up and run `sudo bash -x benchmark-build-agent-setup.sh`
The script takes 4 parameters:
  1. `SERVER_NAME` : e.g., `"vildvittra42"`
  2. `TEAMCITY_ROOT_DIR` : `/ssd`
  3. `ACCESS_KEY_ID` : any valid AWS access key, e.g., your own. it will only be used once, during setup.
  4. `SECRET_ACCESS_KEY` : any valid AWS access key, e.g., your own. it will only be used once, during setup.

All Vildvittra machines have an ssh address in the form: `neo@vildvittra{NUMBER}.malmo.neohq.net`

Once the script has completed:
  1. Go to https://build.neohq.net/agents.html?tab=unauthorizedAgents and authorize `${server_name}` in the GUI.
  2. Go do https://build.neohq.net/agentDetails.html?id=${agent_id}&tab=agentCompatibleConfigurations and
     * Set "Current run configuration policy:" to `Run assigned configurations only`
     * Add all JMH benchmark-running builds -- note, do not add JMH tool-building builds, as they should not run on Vildvittra machines.

Next, to setup keys:
  1. `mkdir /home/teamcity/.ssh`
  2. manually copy keys from `/home/teamcity/.ssh` on another (vildvittra) teamcity agent
  3. `chown --recursive teamcity /home/teamcity/.ssh/`
  4. `chgrp --recursive nogroup /home/teamcity/.ssh/`

If it is a `skalleper\d` (LDBC) machine:
  1. `sudo mkdir /storage/ldbc/`
  2. `sudo chown --recursive teamcity:nogroup /storage/ldbc/`


You also need to add AWS keys for s3 artifact storage
SSH on to the machines
  1. `aws configure`
  2. add the configure from 1password for s3-artifact-storage
  3. `sudo cp -r .aws ../teamcity/.aws`
  4. `sudo chown --recursive teamcity: /home/teamcity/.aws`
