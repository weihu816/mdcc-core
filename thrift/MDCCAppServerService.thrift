include "MDCCCommunicationService.thrift"
namespace java edu.ucsb.cs.mdcc.messaging

service MDCCAppServerService {
  bool ping(),
  MDCCCommunicationService.ReadValue read(1:string key),
  map<string, MDCCCommunicationService.ReadValue> read2(1:string table, 2:string key, 3:list<string> columns),
  list<map<string, MDCCCommunicationService.ReadValue>> read3(1:string table, 2:string key_prefix, 3:list<string> columns, 4:string constraintColumn, 5:string constraintValue, 6:string orderColumn, 7:bool isAssending),
  list<MDCCCommunicationService.ReadValue> read4(1:string table, 2:string key_prefix, 3:string projectionColumn, 4:string constraintColumn, 5:i32 lowerBound, 6:i32 upperBound),
  i32 read5(1:string table, 2:string key_prefix, 3:string constraintColumn, 4:i32 lowerBound, 5:i32 upperBound),
  bool commit(1:string transactionId, 2:list<MDCCCommunicationService.Option> options)
}