FROM openjdk:11-jdk-slim

RUN apt-get update && apt-get install -y openssh-server
RUN mkdir /var/run/sshd
RUN echo 'root:root' | chpasswd
RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN echo 'export AWS_SECRET_KEY=fakeKey' >> /root/.bashrc
RUN echo 'export AWS_ACCESS_KEY_ID=fakeKey' >> /root/.bashrc
RUN echo 'export JAVA_HOME=/usr/local/openjdk-11' >> /root/.bashrc
RUN echo "export PATH=/usr/local/openjdk-11/bin:${PATH}" >> /root/.bashrc
EXPOSE 22
EXPOSE 9007
EXPOSE 9080
CMD ["/usr/sbin/sshd", "-D"]
