## macOS

### VirtualBox

```
vagrant box add bento/ubuntu-18.04
vagrant up
```

After starting, login

- username: vagrant
- password: vagrant

Click on "Application -> System -> xfce terminal"

Run 

```
qgis
```

Postgis will be accessible on 10.0.2.2

You might need to run `/vagrant/bin/vboxclient-all.sh` for better desktop experience.

### Docker

1. Download and start XQuartz from https://www.xquartz.org/

2.
```
brew install socat
socat TCP-LISTEN:6000,reuseaddr,fork,bind=localhost UNIX-CLIENT:\"$DISPLAY\"
```

3. 

```
docker run --rm -e DISPLAY=host.docker.internal:0 \
    -it -v $(pwd):/data \
    kartoza/qgis-desktop qgis
```

Postgis will be accessible on host.docker.internal

