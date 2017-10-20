HDFS File Merge Plugin
============

<a href="https://cdap-users.herokuapp.com/"><img alt="Join CDAP community" src="https://cdap-users.herokuapp.com/badge.svg?t=to-utf8-action"/></a> [![Build Status](https://travis-ci.org/hydrator/to-utf8-action.svg?branch=release/1.0)](https://travis-ci.org/hydrator/to-utf8-action) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) <img alt="CDAP Action" src="https://cdap-users.herokuapp.com/assets/cdap-action.svg"/> []()

HDFS File merge plugin that merges small files in HDFS and writes the merged file to a destination.

Build
-----
To build this plugin:

```
   mvn clean package
```    

The build will create a .jar and .json file under the ``target`` directory.
These files can be used to deploy your plugins.

Deployment
----------
You can deploy your plugins using the CDAP CLI:

    > load artifact <target/hdfs-file-merge-<version>.jar config-file <target/hdfs-file-merge-<version>.json>

For example, if your artifact is named 'hdfs-file-merge-<version>':

    > load artifact target/hdfs-file-merge-<version>.jar config-file target/hdfs-file-merge-<version>.json
    
## Mailing Lists

CDAP User Group and Development Discussions:

* `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from 
users, release announcements, and any other discussions that we think will be helpful 
to the users.

## License and Trademarks

Copyright © 2017 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the 
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions 
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.  
