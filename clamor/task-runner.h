#ifndef TASK_RUNNER_H
#define TASK_RUNNER_H

/** 
 * Abstract runner class for programs running on Clamor.
 * Client libraries can implement this interface to enable
 * distributed functionality for new data-parallel languages.
 * 
 * The calling library must implement the run_task function,
 * which takes as input a pointer to code and a pointer
 * to input data. Any additional arguments can be passed
 * as a custom struct.
 * 
 * The run_task implementation must return a pointer to the task result.
 **/

class TaskRunner {
  virtual void* run_task(void* code, void* data, void* args);
}

#endif // TASK_RUNNER_H
