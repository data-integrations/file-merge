/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.hydrator.plugin.action;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Action to concatenate files in HDFS
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("HDFSFileMerge")
@Description("Action to concatenate small files within HDFS")
public class HDFSFileMergeAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(HDFSFileMergeAction.class);

  private HDFSActionConfig config;

  public HDFSFileMergeAction(HDFSActionConfig config) {
    this.config = config;
  }

  @VisibleForTesting
  class FileStatusComparator implements Comparator<FileStatus> {
    @Override
    public int compare(FileStatus o1, FileStatus o2) {
      String path1 = o1.getPath().getName();
      String path2 = o2.getPath().getName();
      int prefixIndex1 = path1.lastIndexOf("-");
      int prefixIndex2 = path2.lastIndexOf("-");
      int compareVal = path1.substring(0, prefixIndex1).compareTo(path2.substring(0, prefixIndex2));
      if (compareVal == 0) {
        String fileNameIndex1  = path1.substring(prefixIndex1 + 1);
        fileNameIndex1 =
          fileNameIndex1.indexOf(".") == -1 ? fileNameIndex1 : fileNameIndex1.substring(0, fileNameIndex1.indexOf("."));

        String fileNameIndex2  = path2.substring(prefixIndex2 + 1);
        fileNameIndex2 =
          fileNameIndex2.indexOf(".") == -1 ? fileNameIndex2 : fileNameIndex2.substring(0, fileNameIndex2.indexOf("."));

        return Integer.compare(Integer.parseInt(fileNameIndex1), Integer.parseInt(fileNameIndex2));
      } else {
        return compareVal;
      }
    }
  }

  @Override
  public void run(ActionContext context) throws Exception {
    Path source = new Path(config.sourcePath);

    Path dest = new Path(config.destPath);

    FileSystem fileSystem = source.getFileSystem(new Configuration());
    fileSystem.mkdirs(dest.getParent());


    // Moving contents of directory
    FileStatus[] listFiles;
    final String filRegex = source.getName();
    final Path srcDir = source.getParent();
    if (!filRegex.isEmpty()) {
      PathFilter filter = new PathFilter() {
        private final Pattern pattern = Pattern.compile(filRegex);

        @Override
        public boolean accept(Path path) {
          return pattern.matcher(path.getName()).matches();
        }
      };

      listFiles = fileSystem.listStatus(srcDir, filter);
    } else {
      listFiles = fileSystem.listStatus(srcDir);
    }

    if (listFiles.length == 0) {
      if (!filRegex.isEmpty()) {
        LOG.warn("Not concatenating any files of type {} from source {}", filRegex, srcDir.toString());
      } else {
        LOG.warn("Not concatenating any files from source {}", srcDir.toString());
      }
      return;
    }

    //create destination directory if necessary
    fileSystem.mkdirs(dest.getParent());

    // order the files
    Arrays.sort(listFiles, new FileStatusComparator());

    byte[] resultByteArray = new byte[0];

    for (FileStatus file: listFiles) {
      LOG.info("Concatenating file {}", file.getPath().getName());
      source = file.getPath();
      try (FSDataInputStream fsDataInputStream = fileSystem.open(file.getPath())) {
        resultByteArray = Bytes.add(resultByteArray, ByteStreams.toByteArray(fsDataInputStream));
      } catch (IOException e) {
        if (!config.continueOnError) {
          throw e;
        }
        LOG.error("Failed to concatenate file {} to {}", source.toString(), dest.toString(), e);
      }
    }
    LOG.info("Size of byte array {} bytes", resultByteArray.length);
    try {
      if (resultByteArray.length > 0) {
        String path = String.format("%s", config.destPath);
        LOG.info("Destination path file at {}", path);
        FSDataOutputStream outputStream = fileSystem.create(new Path(path));
        LOG.info("Created path file at {}", path);
        outputStream.write(resultByteArray);
        outputStream.close();
        LOG.info("Completed writing {}", path);
      }
    } catch (IOException e) {
      if (!config.continueOnError) {
        throw e;
      }
      LOG.error("Failed to concatenate file {} to {}", source.toString(), dest.toString(), e);
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {

  }

  /**
   * Config class that contains all properties necessary to execute an HDFS move command.
   */
  public class HDFSActionConfig extends PluginConfig {
    @Description("The full HDFS path of the directory whose files have to be merged. " +
      "if fileRegex is set, then only files in the source directory matching the wildcard " +
      "regex will be concatenated. Otherwise, all files in the directory will be concatenated. " +
      "files will be lexicographically sorted before merging, " +
      "for the part files, we also sort by the part file index and we assume they are in following format," +
      "format : part-xx-<id> , we sort by increasing values of <id> for the files with same part-xx " +
      "Example path: hdfs://hostname/tmp")
    @Macro
    private String sourcePath;

    @Description("The valid, full HDFS destination path for directory" +
      " in the same cluster where the concatenated file will be written.")
    @Macro
    private String destPath;

    @Description("Indicates if the pipeline should continue if the concatenate process fails")
    @Nullable
    private boolean continueOnError;

    @VisibleForTesting
    HDFSActionConfig(String sourcePath, String destPath, boolean continueOnError) {
      this.sourcePath = sourcePath;
      this.destPath = destPath;
      this.continueOnError = continueOnError;
    }
  }
}
