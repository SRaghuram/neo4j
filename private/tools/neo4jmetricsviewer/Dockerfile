FROM grafana/grafana:latest-ubuntu
USER root
RUN apt-get update

#disable login stuff
ENV GF_AUTH_DISABLE_LOGIN_FORM "true"
ENV GF_AUTH_ANONYMOUS_ENABLED "true"
ENV GF_AUTH_ANONYMOUS_ORG_ROLE "Viewer"
#Viewer Editor Admin

#install json datasouce plugin
RUN grafana-cli plugins install grafana-simple-json-datasource
#copy datasource/dashboards
COPY grafana /etc/grafana

#install csv->json server
RUN apt-get install npm -y
RUN npm install
COPY src /src
WORKDIR /src
#csv->json is started using run.sh