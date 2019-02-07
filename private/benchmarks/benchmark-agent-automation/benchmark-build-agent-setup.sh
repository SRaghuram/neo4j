#!/bin/bash -exu

# to cleanup after the script following command might be used:
# sudo service teamcity-agent stop && sudo rm -rf /opt/teamcity-agent && sudo rm -rf /home/teamcity/.m2/ && sudo rm -f /usr/lib/jvm/oracle-jdk-8

if [ $# -ne 5 ]
  then
    echo "Usage: vildvittra-build-agent-setup.sh SERVER_NAME TEAMCITY_ROOT_DIR ACCESS_KEY_ID SECRET_ACCESS_KEY S3_SECRET_ACCESS_KEY"
    exit 1
fi

server_name=${1}
teamcity_root_dir=${2}
access_key_id=${3}
secret_access_key=${4}
s3_secret_access_key=${5}

apt-get -y install unzip
apt-get -y install python-pip
pip install --upgrade pip
pip install awscli
apt-get -y install make
apt-get -y install g++

# install git
apt-get -y install git
git config --global user.email "continuous-integration+build-agent@neotechnology.com"
git config --global user.name "Neo Technology Build Agent"

# install oracle jdk 8 -- start
apt-get -y install software-properties-common
apt-get -y install python-software-properties

echo 'oracle-java8-installer shared/accepted-oracle-license-v1-1 boolean true' | debconf-set-selections

add-apt-repository ppa:webupd8team/java -y
apt-get update
apt-get -y install --quiet oracle-java8-installer

# teamcity requires oracle jdk to have a specific non-default path
ln -s /usr/lib/jvm/java-8-oracle /usr/lib/jvm/oracle-jdk-8

# Do not set JAVA_HOME in /etc/profile. It should already be set in /etc/profile.d/*.sh
grep -v JAVA_HOME /etc/profile > /tmp/profile.fixed
cat /tmp/profile.fixed > /etc/profile
rm -f /tmp/profile.fixed
source /etc/profile  # This ensures that the correct environment is available for other installations below
# install oracle jdk 8 -- end

# install oracle jdk 10 -- start
echo "oracle-java10-installer shared/accepted-oracle-license-v1-1 boolean true" | debconf-set-selections

add-apt-repository ppa:linuxuprising/java -y
apt update
apt -y install --quiet oracle-java10-installer

# teamcity requires oracle jdk to have a specific non-default path
ln -s /usr/lib/jvm/java-10-oracle /usr/lib/jvm/oracle-jdk-10
# install oracle jdk 10 -- end

adduser --system teamcity

# install teamcity build agent -- start

# find the latest buildAgent-X.Y.Z.zip file
{
    # try
    export AWS_ACCESS_KEY_ID=${access_key_id}
    export AWS_SECRET_ACCESS_KEY=${secret_access_key}

    latest_build_agent_zip=$(aws --region eu-west-1 s3 ls s3://build-service.neo4j.org/buildAgent | tr -s ' ' | sort -t ' ' -k1,1 -k2,2 | tail -1 | cut -d ' ' -f4)
    aws --region eu-west-1 s3 cp s3://build-service.neo4j.org/"${latest_build_agent_zip}" ./

    export AWS_ACCESS_KEY_ID=""
    export AWS_SECRET_ACCESS_KEY=""
} || {
    # catch
    export AWS_ACCESS_KEY_ID=""
    export AWS_SECRET_ACCESS_KEY=""

    echo "AWS cli failed"
    exit 1
}

mkdir /opt/teamcity-agent
unzip "${latest_build_agent_zip}" -d /opt/teamcity-agent
chmod +x /opt/teamcity-agent/bin/agent.sh
cp /opt/teamcity-agent/conf/buildAgent.dist.properties /opt/teamcity-agent/conf/buildAgent.properties
chown --recursive teamcity: /opt/teamcity-agent
# install teamcity build agent -- end

# install maven -- start
apt-get -y install maven
mkdir /home/teamcity/.m2
chown --recursive teamcity: /home/teamcity/.m2
# install maven -- end

# create teamcity systemd script
teamcity_systemd_script=/etc/systemd/system/teamcity-agent.service
cat > ${teamcity_systemd_script} <<- EOM
[Unit]
Description=TeamCity Agent

[Service]
LimitNOFILE=262140
User=teamcity
ExecStart=/opt/teamcity-agent/bin/agent.sh run

[Install]
WantedBy=multi-user.target
EOM

# enable teamcity agent service
systemctl daemon-reload
systemctl enable teamcity-agent.service

# create teamcity agent properties file
agent_properties_file=/opt/teamcity-agent/conf/buildAgent.properties
cp /opt/teamcity-agent/conf/buildAgent.dist.properties ${agent_properties_file}

teamcity_work_dir=${teamcity_root_dir}/work
teamcity_temp_dir=${teamcity_root_dir}/temp
teamcity_system_dir=${teamcity_root_dir}/system

mkdir "${teamcity_work_dir}"
mkdir "${teamcity_temp_dir}"
mkdir "${teamcity_system_dir}"

sed -i "/name=/c\\name=${server_name}" ${agent_properties_file}
sed -i "/serverUrl=/c\\serverUrl=https://live.neo4j-build.io" ${agent_properties_file}
sed -i "/workDir=/c\\workDir=${teamcity_work_dir}" ${agent_properties_file}
sed -i "/tempDir=/c\\tempDir=${teamcity_temp_dir}" ${agent_properties_file}
sed -i "/systemDir=/c\\systemDir=${teamcity_system_dir}" ${agent_properties_file}

chown --recursive teamcity: "${teamcity_work_dir}"
chown --recursive teamcity: "${teamcity_temp_dir}"
chown --recursive teamcity: "${teamcity_system_dir}"

service teamcity-agent start

#
# Install benchmarking tools - begin
#
echo -e "\\nInstalling FlameGraph, JFR, Async-Profiler support\\n"

profiling_root=/opt/profiling
export FLAMEGRAPH_DIR=${profiling_root}/FlameGraph
export JFR_FLAMEGRAPH=${profiling_root}/jfr-flame-graph
export ASYNC_PROFILER_DIR=${profiling_root}/async-profiler

# configure environment variables
benchmark_profile=/etc/profile.d/benchmark-profiling.sh
rm -f ${benchmark_profile}
if ! touch ${benchmark_profile}
then
  echo "Cannot edit profile.d without root permission"
  exit -1
fi

{
echo "export FLAMEGRAPH_DIR=$FLAMEGRAPH_DIR"
echo "export JFR_FLAMEGRAPH=$JFR_FLAMEGRAPH"
echo "export ASYNC_PROFILER_DIR=$ASYNC_PROFILER_DIR"
} >> ${benchmark_profile}

echo "JAVA_HOME=$JAVA_HOME"
echo "FLAMEGRAPH_DIR=$FLAMEGRAPH_DIR"
echo "JFR_FLAMEGRAPH=$JFR_FLAMEGRAPH"
echo "ASYNC_PROFILER_DIR=$ASYNC_PROFILER_DIR"

# Install FlameGraph and jfr-flame-graph
mkdir -p ${profiling_root}
git clone https://github.com/brendangregg/FlameGraph.git ${FLAMEGRAPH_DIR}
git clone https://github.com/chrishantha/jfr-flame-graph.git ${JFR_FLAMEGRAPH}
(
  cd ${JFR_FLAMEGRAPH}
  ./install-mc-jars.sh
  mvn clean install -U
  chmod +x create_flamegraph.sh
)

# configure sysctl variables for async profiler
benchmark_sysctl=/etc/sysctl.d/benchmark-profiling.conf
rm -f ${benchmark_sysctl}

if ! touch ${benchmark_sysctl}
then
  echo "Cannot edit sysctl files without root permission"
  exit -1
fi
echo "kernel.perf_event_paranoid=1" >> ${benchmark_sysctl}
echo "kernel.kptr_restrict=0" >> ${benchmark_sysctl}

# reload sysctl configuration
sysctl -p /etc/sysctl.conf

# install async profiler
git clone https://github.com/jvm-profiling-tools/async-profiler.git ${ASYNC_PROFILER_DIR}
cd ${ASYNC_PROFILER_DIR}
make

chown --recursive teamcity: /opt/profiling/
#
# Install benchmarking tools - end
#
# Installing AWS s3 key.

aws configure set aws_access_key_id AKIAIWW4OUVMCVBAOIJQ
aws configure set aws_secret_access_key "${s3_secret_access_key}"
aws configure set default.region eu-west-1
cd ~
sudo cp -r .aws ../teamcity/.aws
sudo chown --recursive teamcity: /home/teamcity/.aws

echo "Refer to guide for next steps"
echo "--> https://github.com/neo-technology/build-service/blob/master/vildvittra-build-agent-setup-guide.md"
