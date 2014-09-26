namespace java edu.ucsb.cs.mdcc.messaging

struct BallotNumber {
  1:i64 number,
  2:string processId
}

struct ReadValue {
  1:string key
  2:i64 version
  3:i64 classicEndVersion
  4:binary value
}

struct Accept {
  1:string transactionId
  2:BallotNumber ballot
  3:string key
  4:i64 oldVersion
  5:binary newValue
}

struct Option {
  1:string key
  2:i64 oldVersion
  3:binary value
  4:bool classic
}

service MDCCCommunicationService {

  bool ping(),
  
  bool prepare(1:string key, 2:BallotNumber ballot, 3:i64 classicEndVersion),

  bool accept(1:Accept accept),
  
  list<bool> bulkAccept(1:list<Accept> accepts),
  
  bool runClassic(1:string transaction, 2:string key, 3:i64 oldVersion, 4:binary newValue),
  
  void decide(1:string transaction, 2:bool commit),
  
  ReadValue read(1:string key),
  
  map<string, ReadValue> read2(1:string table, 2:string key, 3:list<string> columns),
  
  list<map<string, ReadValue>> read3(1:string table, 2:string key_prefix, 3:list<string> columns, 4:string constraintColumn, 5:string constraintValue, 6:string orderColumn, 7:bool isAssending),
  
  list<ReadValue> read4(1:string table, 2:string key_prefix, 3:string projectionColumn, 4:string constraintColumn, 5:i32 lowerBound, 6:i32 upperBound),
  
  i32 read5(1:string table, 2:string key_prefix, 3:string constraintColumn, 4:i32 lowerBound, 5:i32 upperBound),
  
  map<string,ReadValue> recover(1:map<string,i64> versions)

}