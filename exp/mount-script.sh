#!/bin/bash
ulimit -n 655350
sudo cp /etc/fstab /etc/fstab.orig
for i in `seq 0 3`
do 
	sudo mkfs -t xfs /dev/nvme${i}n1
	sudo mkdir /data${i}
	sudo mount /dev/nvme${i}n1 /data${i}
	sudo echo "$(blkid -o export /dev/nvme${i}n1 | grep ^UUID=) /data${i}/ xfs defaults,noatime" | tee -a /etc/fstab
	sudo cp -r ~/partitionforjoin/ /data${i}/
	sudo chown -R ubuntu /data${i}/partitionforjoin/
done
