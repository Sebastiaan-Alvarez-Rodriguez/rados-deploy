#!/bin/bash
#Author: Jayjeet Chakraborty
#Url: https://github.com/JayjeetAtGithub/skyhook-aws/blob/master/scripts/launch-vms.sh
set -e

count=7
ami=ami-0d527b8c289b4af7f
instance_type=c5d.12xlarge
security_group_ids=sg-0e872cb135d3da2e0
subnet_id=subnet-c541d289
key_name=yuxuan_frankfurt
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
