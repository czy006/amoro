namespace java org.apache.amoro.api

include "amoro_commons.thrift"

struct ExecutorTaskResult {
    1: i64 processId;
    2: i64 tableId;
    3: amoro_commons.TableIdentifier tableIdentifier;
    6: string action;
    7: i32 status;
    8: i32 retryNumber;
    9: optional binary taskOutput;
    10: optional string errorMessage;
    11: optional map<string, string> summary;
}

service MaintainerService {

  void ping()

  void ackTask(1: ExecutorTaskResult taskResult) throws(1: amoro_commons.AmoroException e1)

  void completeTask(1: ExecutorTaskResult taskResult) throws (1: amoro_commons.AmoroException e1)

}