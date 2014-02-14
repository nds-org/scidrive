SciDrive - scientific cloud storage
================================

SciDrive is a REST-service supporting [VOSpace 2.0](http://www.ivoa.net/documents/VOSpace/) and [Dropbox](https://www.dropbox.com/developers/core/docs)
protocols to access the data. The primary data storage platform is [OpenStack SWIFT](http://docs.openstack.org/developer/swift/).

The project is based on Jersey REST framework and provides the REST service allowing to store the data in a cloud storage.
The service allows extracting metadata from known filetypes to store it in 3rd-party services.

There are public service and web portal running at http://www.scidrive.org

Quick start
-----------
1. Install OpenStack SWIFT cluster using [SWIFT](https://launchpad.net/swift) binaries with SWAuth authentication
2. Setup Apache Tomcat, MySQL and RabbitMQ
3. Create a new database in MySQL using the scidrive/vospace.sql script
4. Make changes to the scidrive/src/application.properties file. There is documentation on the application properties in [Wiki](https://github.com/dimm0/scidrive/wiki/SciDrive-Configure)
5. Generate a pool of SWIFT users using the scidrive/sql/generate_swift_users.pl script
6. Use Apache Ant to create the war file and deploy it to Tomcat: `ant deploy`
7. Install the [SciDrive web UI](https://github.com/dimm0/scidrive-ui) to start using this installation.
8. You can use the [Python command-line tool](https://github.com/dimm0/scidrive-python-client) to access your data from command line

Developing in Eclipse
------------------
1. Clone the code from github: git clone https://github.com/dimm0/scidrive.git, or use GUI
2. Import the source code into Eclipse: File->New, Dynamic Web Project
3. In Web Module dialog, choose:
  * Context root: vospace-2.0
  * Content directory: web
  * Uncheck the 'Create web.xml deployment descriptor'
4. Right-click build.xml file, Run As->Ant Build..., check "resolve" task only, click Run
  * This will pull all needed libs from Maven repo and store them in lib folder
5. Manually copy libs or create a symlink of lib folder to web/WEB-INF folder, f.e.:
  * ln -s ~/Documents/workspace/scidrive/lib ~/Documents/workspace/scidrive/web/WEB-INF/lib
  * Copy two libs from lib.local to web/WEB-INF/lib
6. Add all libraries from lib folder to Eclipse project build path:
  * Select lib/*.jar files in Project explorer, right click, Build Path->Add to Build path
  * Do the same for lib.local libs
7. Convert project to facetes:
  * Go to Project->Properties, Project Facetes. Make sure it's Facet form, if not enable it.
  * Should have Dynamic Web Module enabled
8. If needed configure Tomcat server in Project->Properties, Server
9. Run the web app:
  * Right-click on scdive project in Project Explorer, Run As->Run on Server
10. When properly configured (application.properties file in right location, pointing to right database, etc) should show server log for running SciDrive REST resources:
```
Feb 14, 2014 1:30:20 PM com.sun.jersey.api.core.PackagesResourceConfig init
INFO: Scanning for root resource and provider classes in the packages:
  edu.jhu.pha.vosync.rest
  edu.jhu.pha.vospace.rest
  edu.jhu.pha.vospace.oauth
Feb 14, 2014 1:30:21 PM com.sun.jersey.api.core.ScanningResourceConfig logClasses
INFO: Root resource classes found:
  class edu.jhu.pha.vospace.oauth.RequestTokenRequest
  class edu.jhu.pha.vospace.rest.NodesController
  class edu.jhu.pha.vospace.oauth.AccessTokenRequest
  class edu.jhu.pha.vosync.rest.DropboxService
  class edu.jhu.pha.vospace.rest.MetadataController
  class edu.jhu.pha.vospace.rest.DataController
  class edu.jhu.pha.vospace.rest.TransfersController
```
