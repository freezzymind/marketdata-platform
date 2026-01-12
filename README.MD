# Kubernetes Single-Node Cluster on Debian 12 via Ansible

## Purpose
This Ansible block provisions a **single-node Kubernetes cluster** on **Debian 12 (Bookworm)** with the **containerd** container runtime, **Calico CNI**, a local **NGINX stream proxy** to the Kubernetes API server, and applies **network hardening** (Fail2Ban, sysctl tuning, SSH, nftables + UFW).

## Goal
The goal is to **quickly and reproducibly** deploy a functional Kubernetes cluster for a project, with **host-level protection**, and a **predictable, controlled network layout**.

## Requirements

### Target host
Debian 12 (Bookworm)
Root SSH access (recommended: SSH key auth)
Internet access (APT repos + Calico manifest fetch)

### Control node
Ansible installed
Ability to SSH to the target host as root

## How To Run
ansible-playbook -i inventory/hosts.local.ini playbook.yml

## Execution Order (Playbook)
playbook.yml runs on hosts: master in this order:

- 0_setup_internal_interface
- 1_set_system_hostname
- 2_python_setup
- 3_base_packages
- 4_prepare_host_for_k8s
- 5_containerd
- 6_kubernetes
- 7_init_cluster
- 8_cni_calico
- 9_nginx_setup
- 10_network_hardening



## Role-by-Role Breakdown (Detailed)

## 0_setup_internal_interface — Internal dummy interface for API addressing

### What it does:
Enables and starts systemd-networkd
Creates a dummy interface kubeint0
Assigns 10.200.0.1/32 to kubeint0
Adds a blackhole route for 10.200.0.0/24
Installs and enables a systemd oneshot service to persist route:
ip route add 10.200.0.1 dev kubeint0

### Files deployed:
/etc/systemd/network/10-kubeint0.netdev
/etc/systemd/network/10-kubeint0.network
/etc/systemd/system/route-kubeint0.service

### Why:
Ensures a stable, controlled internal endpoint (10.200.0.1) for Kubernetes API binding/advertising.


## 1_set_system_hostname — Hostname and minimal /etc/hosts

### What it does:
Sets hostname: {{ hostname }}
Replaces /etc/hosts with a minimal IPv4-only mapping:
{{ public_ip }} {{ hostname }}
{{ apiserver_advertise_address }} {{ hostname }}
127.0.0.1 {{ hostname }}
127.0.0.1 localhost

### Files deployed:
/etc/hostname (via Ansible module)
/etc/hosts (full overwrite)


## 2_python_setup — Python and automation dependencies

### What it does:
Installs Python 3 + pip and a set of packages used for Kubernetes and automation tooling:
python3, python3-pip, python3-venv
python3-kubernetes, python3-openshift, python3-yaml, python3-jsonpatch
python3-requests, python3-passlib
DB/queue libs: python3-psycopg2, python3-redis
python3-docker


## 3_base_packages — Core OS tooling for ops/debug

### What it does:
Installs common operational utilities:
diagnostics/network: net-tools, traceroute, mtr, dnsutils, iperf3, tcpdump, ethtool
json/cli: jq
crypto/certs: openssl, ca-certificates, gnupg2
k8s prereqs: conntrack, socat, ebtables, iptables
utils/editors: git, nano, rsync, archives tooling


## 4_prepare_host_for_k8s — Kernel modules, sysctl, swap disable

### What it does:
Ensures kernel modules load at boot:
overlay
br_netfilter
Loads them immediately (modprobe)
Sets Kubernetes sysctl:
net.bridge.bridge-nf-call-iptables = 1
net.ipv4.ip_forward = 1
Creates required directories:
/etc/cni/net.d
/etc/apt/keyrings
/etc/containerd
Disables swap:
swapoff -a
comments swap entries in /etc/fstab

### Files deployed:
/etc/modules-load.d/k8s.conf
/etc/sysctl.d/99-kubernetes-cri.conf
/etc/fstab (swap lines commented)


## 5_containerd — Install and configure containerd (CRI)

### What it does:
Adds Docker GPG key → /etc/apt/keyrings/docker.gpg
Adds Docker apt repo for Debian Bookworm
Installs containerd
Deploys /etc/containerd/config.toml with:
sandbox_image = {{ sandbox_image }}
CNI:
bin_dir = /opt/cni/bin
conf_dir = /etc/cni/net.d
max_conf_num = 1
runtime:
SystemdCgroup = true
metrics:
address = 127.0.0.1:{{ containerd_metrics_port }}
Restarts and enables containerd

### Files deployed:
/etc/apt/sources.list.d/docker.list
/etc/containerd/config.toml


## 6_kubernetes — Install kubelet/kubeadm/kubectl (v1.30 repo)

### What it does:
Adds Kubernetes apt key:
/etc/apt/keyrings/kubernetes-apt-keyring.gpg
Adds apt repo (v1.30) from download.opensuse.org
Installs:
kubelet
kubeadm
kubectl
Pins versions:
apt-mark hold kubelet kubeadm kubectl


## 7_init_cluster — kubeadm init + kubeconfig

### What it does:
Initializes the control plane:
kubeadm init with:
--apiserver-advertise-address={{ apiserver_advertise_address }}
--pod-network-cidr={{ pod_network_cidr }}
--service-cidr={{ service_cidr }}
--cri-socket unix:///run/containerd/containerd.sock
--apiserver-cert-extra-sans={{ apiserver_cert_extra_sans }}
Ensures API server binds to:
--bind-address={{ bind_address }} (in manifest patch)
Copies admin kubeconfig:
/etc/kubernetes/admin.conf -> ~/.kube/config

### Files deployed:
/etc/kubernetes/admin.conf (created by kubeadm)
/etc/kubernetes/manifests/kube-apiserver.yaml (patched)
~/.kube/config


## 8_cni_calico — Apply Calico and make node schedulable

### What it does:
Applies Calico manifest:
https://raw.githubusercontent.com/projectcalico/calico/v3.27.3/manifests/calico.yaml
with retries
Waits until all pods in all namespaces become Ready
Removes control-plane taints (single-node scheduling):
node-role.kubernetes.io/control-plane-
node-role.kubernetes.io/master-
Prints pod status (kubectl get pods -A)


## 9_nginx_setup — Local stream proxy to kube-apiserver

### What it does:
Installs:
nginx
libnginx-mod-stream
Deploys stream-only nginx config:
/etc/nginx/nginx.conf
Removes default site:
/etc/nginx/sites-enabled/default
Deploys stream config:
listen 127.0.0.1:6443; proxy_pass <apiserver_advertise_address>:6443;
Restarts/enables nginx

### Files deployed:
/etc/nginx/nginx.conf
/etc/nginx/conf.d/kubeapi.stream

### Result
Kubernetes API accessible locally via:
https://127.0.0.1:6443


## 10_network_hardening — Fail2Ban + SSH hardening + sysctl + nftables + UFW

### 10.1 Fail2Ban (SSH)
Installs fail2ban
Deploys /etc/fail2ban/jail.local:
ignoreip = {{ ignoreip }}
maxretry = 1, findtime = 60, bantime = -1 (permanent)
sshd and sshd-ddos enabled
Restarts/enables Fail2Ban

### 10.2 OpenSSH hardening
Installs openssh-server
Deploys hardened /etc/ssh/sshd_config:
PasswordAuthentication no
PubkeyAuthentication yes
MaxAuthTries 2, LoginGraceTime 10
AllowAgentForwarding no
AllowTcpForwarding local
modern crypto constraints (ciphers/kex/macs)
IPv4 only (AddressFamily inet)
Adds systemd resource limits override:
/etc/systemd/system/sshd.service.d/limits.conf
LimitNOFILE=65536, etc.
daemon-reload + restart/enables sshd

### 10.3 sysctl hardening
Deploys:
/etc/sysctl.d/100-hardening.conf:
rp_filter, redirects off, martians logging
SYN flood mitigation, tcp tuning
IPv6 disabled
/etc/sysctl.d/102-conntrack.conf:
increased conntrack max/buckets
reduced SYN_RECV + TIME_WAIT timeouts
Applies with:
sysctl --system

### 10.4 nftables pre-filter
Installs and enables nftables
Creates /etc/nftables.d
Deploys pre-filter rule:
loopback allow
established/related allow
invalid drop
minimal ICMP allow
Deploys /etc/nftables.conf including /etc/nftables.d/*.conf
Applies: nft -f /etc/nftables.conf

### 10.5 UFW baseline firewall
Installs UFW
Sets policies:
incoming: deny
outgoing: allow
routed: allow
Allows SSH TCP 22 + enables rate limit
Denies SSH UDP 22
Enables firewall + starts/enables ufw service



## Post-Install Verification

Run on the target host:

### Kubernetes:
kubectl cluster-info
kubectl get nodes -o wide
kubectl get pods -A

### containerd:
systemctl status containerd --no-pager
ss -lntp | grep 1338
containerd metrics port (default from vars)

### NGINX stream proxy:
ss -lntp | grep 6443 
should show nginx listening on 127.0.0.1:6443

### Firewall & hardening:
ufw status verbose
systemctl status fail2ban --no-pager
fail2ban-client status sshd
nft list ruleset
sysctl net.ipv4.tcp_syncookies net.ipv6.conf.all.disable_ipv6



## Notes / Operational Implications

IPv6 is disabled by sysctl hardening. If you need IPv6, remove/override:
net.ipv6.conf.all.disable_ipv6 = 1
net.ipv6.conf.default.disable_ipv6 = 1
Fail2Ban is extremely strict (maxretry=1, permanent ban). Make sure ignoreip is correct, otherwise you can lock yourself out.
/etc/hosts is overwritten (not appended). Do not keep manual edits there.
Kubernetes packages are held (apt-mark hold). Upgrades require explicit unhold/upgrade flow.
Calico is applied from GitHub raw manifest. If you need pinning via local file, replace the URL with a committed manifest.


