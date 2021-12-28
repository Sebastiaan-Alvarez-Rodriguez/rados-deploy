#!/bin/bash
#Author: Jayjeet Chakraborty
#Url: https://github.com/JayjeetAtGithub/skyhook-aws/blob/master/scripts/launch-vms.sh
set -e

count=2
ami=ami-0fdf70ed5c34c5f52
instance_type=m5ad.2xlarge
security_group_ids=sg-04c41bc658406a187
subnet_id=subnet-0e67a4762ad1c5e8c
key_name=yuxuan
chmod 400 "${key_name}.pem"

spawn_ec2_instances() {
    echo "[+] Launching $count ec2 instances"
    aws ec2 run-instances \
        --image-id $ami \
        --count $count \
        --instance-type $instance_type	 \
        --key-name $key_name \
        --security-group-ids $security_group_ids \
        --subnet-id $subnet_id

    sleep 60

    echo "[+] Gathering Public and Private IPs "
    echo " " > public_ips.txt
    echo " " > private_ips.txt
    aws ec2 describe-instances --output text --query "Reservations[].Instances[].NetworkInterfaces[].Association.PublicIp" > public_ips.txt
    aws ec2 describe-instances --output text --query "Reservations[].Instances[].NetworkInterfaces[].PrivateIpAddress" > private_ips.txt
}

case "$1" in
    -s|--spawn)
    spawn_ec2_instances
    ;;
    *)
    echo "Usage: (-s|--spawn)"
    exit 0
    ;;
esac
