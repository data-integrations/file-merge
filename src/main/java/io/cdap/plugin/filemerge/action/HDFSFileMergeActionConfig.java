/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.filemerge.action;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import javax.annotation.Nullable;

/**
 * Config class for {@link HDFSFileMergeAction}.
 */
public class HDFSFileMergeActionConfig extends PluginConfig {
  public static final String SOURCE_PATH = "sourcePath";
  public static final String DEST_PATH = "destPath";
  public static final String CONTINUE_ON_ERROR = "continueOnError";

  @Name(SOURCE_PATH)
  @Description("The full HDFS path of the directory whose files have to be merged. " +
    "if fileRegex is set, then only files in the source directory matching the wildcard " +
    "regex will be concatenated. Otherwise, all files in the directory will be concatenated. " +
    "files will be lexicographically sorted before merging, " +
    "for the part files, we also sort by the part file index and we assume they are in following format," +
    "format : part-xx-<id> , we sort by increasing values of <id> for the files with same part-xx " +
    "Example path: hdfs://hostname/tmp")
  @Macro
  private final String sourcePath;

  @Name(DEST_PATH)
  @Description("The valid, full HDFS destination path for directory" +
    " in the same cluster where the concatenated file will be written.")
  @Macro
  private final String destPath;

  @Name(CONTINUE_ON_ERROR)
  @Description("Indicates if the pipeline should continue if the concatenate process fails")
  @Nullable
  private final boolean continueOnError;

  public HDFSFileMergeActionConfig(String sourcePath, String destPath, boolean continueOnError) {
    this.sourcePath = sourcePath;
    this.destPath = destPath;
    this.continueOnError = continueOnError;
  }

  public String getSourcePath() {
    return sourcePath;
  }

  public String getDestPath() {
    return destPath;
  }

  public boolean isContinueOnError() {
    return continueOnError;
  }

  public void validate(FailureCollector collector) {
    if (!containsMacro(SOURCE_PATH) && Strings.isNullOrEmpty(sourcePath)) {
      collector.addFailure("Source path must be specified.", null).withConfigProperty(SOURCE_PATH);
    }

    if (!containsMacro(DEST_PATH) && Strings.isNullOrEmpty(destPath)) {
      collector.addFailure("Destination path must be specified.", null).withConfigProperty(DEST_PATH);
    }
  }
}
