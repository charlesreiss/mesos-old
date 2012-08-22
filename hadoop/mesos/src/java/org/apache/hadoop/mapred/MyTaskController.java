package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class MyTaskController extends DefaultTaskController {
  @Override
  public int launchTask(String user, 
                                  String jobId,
                                  String attemptId,
                                  List<String> setup,
                                  List<String> _jvmArguments,
                                  File currentWorkDirectory,
                                  String stdout,
                                  String stderr) throws IOException {
    ArrayList<String> jvmArguments = new ArrayList<String>();
    jvmArguments.add("nice");
    jvmArguments.addAll(_jvmArguments);
    return super.launchTask(user, jobId, attemptId, setup, jvmArguments,
        currentWorkDirectory, stdout, stderr);
  }
}
