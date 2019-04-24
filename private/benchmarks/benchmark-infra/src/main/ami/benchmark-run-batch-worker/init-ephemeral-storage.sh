#!/usr/bin/env bash

partition=/dev/nvme1n1
# Format, if it does not contain a partition yet
if [ "$(file -b -s $partition)" == "data" ]; then
  mkfs -t ext4 $partition
  mkdir -p /work
  mount $partition /work
  # Persist the volume in /etc/fstab so it gets mounted again
  echo "$partition /work ext4 defaults,nofail 0 2" >> /etc/fstab
fi
