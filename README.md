VOBox - scientific cloud storage
================================

VOBox is a REST-service supporting [VOSpace 2.0](http://www.ivoa.net/documents/VOSpace/) and [Dropbox](https://www.dropbox.com/developers/core/docs)
protocols to access the data. The primary data storage platform is [OpenStack SWIFT](http://docs.openstack.org/developer/swift/).

The project is based on Jersey REST framework and provides the REST service allowing to store the data in a cloud storage.
The service allows extracting metadata from known filetypes to store it in 3rd-party services.

Quick start
-----------
1. Install OpenStack SWIFT cluster using [SWIFT](https://launchpad.net/swift) binaries with SWAuth authentication
2. Setup Apache Tomcat, MySQL and RabbitMQ
3. Create a new database in MySQL using the vobox/vospace.sql script
4. Make changes to the vobox/src/application.properties file. There is documentation on the application properties in [Wiki](https://github.com/dimm0/vobox/wiki/Vobox-Configure)
5. Generate a pool of SWIFT users using the vobox/sql/generate_swift_users.pl script
6. Use Apache Ant to create the war file and deploy it to Tomcat: `ant deploy`
7. Install the [VOBox web UI](https://github.com/dimm0/vobox-ui) to start using this installation.
8. You can use the [Python command-line tool](https://github.com/dimm0/vobox-python-client) to access your data from command line
