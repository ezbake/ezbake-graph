# EzGraph
### Basics

EzGraph is a thrift service around Titan (https://github.com/thinkaurelius/titan) that implements a Thrift IDL (${path.to.ezbake.common.services.thrift}/src/main/thrift/EzGraphService.thrift).


### Background

The underlying implementation of Titan is a version written in-house: http://github.com/ezbake/titan. This version of Titan utilizes Accumulo and Titan-Secure ElasticSearch for its backend data store and indexing solution. Titan was migrated to get it setup on Jenkins and artifactory for continuous integration.

A rudimentary sample driver for the graph service exists here: http://github.com/ezbake/ezbake-graph-client.

### Running on EzCentos

```mvn clean install –U ``` installs jars to /vagrant/ezbakejars/

```
vagrant reload --provision (start security service)
vagrant ssh
sudo su
```

```
/vagrant/scripts/startHadoop.sh
/vagrant/scripts/startAccumulo.sh
/vagrant/scripts/startRedis.sh
sudo service ElasticSearch start

/vagrant/scritpts/startService.sh graph-service
```

let’s you know when it’s started:

```
tail -f /tmp/ezcentos-apps/common_services/graph-service.log
ps aux | grep graph
```

### Debugging


Running the debugger: ```/vagrant/scripts/DEBUGGER.sh useDebug 5005```
