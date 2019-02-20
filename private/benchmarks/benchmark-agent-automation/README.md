# how to run it ?

First make sure that you have [Ansible 2.6.x](https://docs.ansible.com/ansible/2.6/installation_guide/intro_installation.html) installed.
Once it is in, it is simple as this:

  ansible-playbook -i inventory install-profilers.yaml

if you don't use `ssh-agent` to manage keys you can pass private key directly
to `ansible-playbook` command.

  ansible-playbook --private-key [path to private key] -i inventory install-profilers.yaml

# install profilers on local machine

At the moment it is shell script, but will make it ansible playbook soon, so stay tuned.

You just call it like this (remember to set JAVA_HOME variable):

	./install-profilers.sh [your tools directory]