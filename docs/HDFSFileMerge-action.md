# HDFS File Merge Action


Description
-----------
Merges small files within an HDFS cluster.


Use Case
--------
This action can be used when two or more small files need to be merged and written to a new location in an HDFS cluster.


Properties
----------
**sourcePath:** The full HDFS path of the directory whose files have to be merged. if fileRegex is set,
                then only files in the source directory matching the wildcard
                regex will be merged. Otherwise, all files in the directory will be merged.
                files will be lexicographically sorted before merging, for the part files,
                we also sort by the part file index and we assume they are in following format,
                format : `part-xx-<id>` , we sort by increasing values of `<id>` for the files with same `part-xx`
                Example path: `hdfs://hostname/tmp`.


**destPath:** The valid, full HDFS destination directory in the same cluster where the merged file will be written.
the directory will be created if it doesn't exist already.

**destFileName** Optional Name of the merged file, default is `output` if no value not provided.

**fileRegex:** Wildcard regular expression to filter the files in the source directory that will be moved.
Example to match all avro files `.*\.avro`

**continueOnError:** Indicates if the pipeline should continue if the move process fails. If all files are not
successfully moved, the action will not return the files already moved to their original locations.


Example
-------
This example merges avro files from `/source/path` and writes the merged file `merge-output.avro` to `/dest/path`:

        {
           "name": "HDFSFileMerge",
           "plugin": {
               "name": "HDFSFileMerge",
               "type": "action",
               "label": "HDFSFileMerge",
               "artifact": {
                   "name": "hdfs-file-merge",
                   "version": "1.0.0-SNAPSHOT",
                   "scope": "SYSTEM"
               },
               "properties": {
                   "continueOnError": "false",
                   "sourcePath": "hdfs://example.net/source/path/",
                   "destPath": "hdfs://example.net/dest/path",
                   "fileRegex": ".*\\.avro",
                   "destFileName": "merge-output.avro"
               }
           },
           "outputSchema": ""
        }
