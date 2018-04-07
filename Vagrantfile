# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  config.vm.box = "bento/ubuntu-18.04"
  config.vm.box_check_update = false

  config.vm.provider "virtualbox" do |vb|
    vb.memory = "4096"
    vb.cpus = 2
    vb.gui = true
  end
  config.vm.provision "shell", inline: <<-SHELL
    apt-get update
    apt-get install -y \
      vim \
      less \
      jq \
      unzip \
      xfce4 \
      xfce4-terminal \
      virtualbox-guest-dkms \
      virtualbox-guest-utils \
      virtualbox-guest-x11 \
      gdm \
      qgis
    sed -i'' -e '/^allowed/s/console/anybody/' /etc/X11/Xwrapper.config
 SHELL
end
