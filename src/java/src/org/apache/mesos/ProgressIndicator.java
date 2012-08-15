
package org.apache.mesos;

import org.apache.mesos.Protos.*;

public interface ProgressIndicator {
  void requestProgress(ExecutorDriver driver);
}
