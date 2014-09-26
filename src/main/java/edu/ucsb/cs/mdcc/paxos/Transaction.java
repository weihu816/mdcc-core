package edu.ucsb.cs.mdcc.paxos;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.Result;
import edu.ucsb.cs.mdcc.dao.Database;

import java.util.*;
import java.util.Map.Entry;

public abstract class Transaction {

    protected String transactionId;
    protected boolean complete;

    private Map<String,Result> readSet = new HashMap<String, Result>();
    protected Map<String,Option> writeSet = new HashMap<String, Option>();

    public void begin() {
        this.transactionId = UUID.randomUUID().toString();
    }

    public synchronized byte[] read(String key) throws TransactionException {
        assertState();

        if (readSet.containsKey(key)) {
            Result result = readSet.get(key);
            if (result.isDeleted()) {
                throw new TransactionException("No object exists by the key: " + key);
            }
            if (result.getVersion() == 0) {
                throw new TransactionException("No object exists by the key: " + key);
            }
            return result.getValue();
        } else {
            Result result = doRead(key);
            if (result != null) {
                result.setDeleted(isDeleted(result.getValue()));
                readSet.put(result.getKey(), result);

                if (result.isDeleted()) {
                    throw new TransactionException("No object exists by the key: " + key);
                }

                if (result.getVersion() == 0) {
                    throw new TransactionException("No object exists by the key: " + key);
                }
                return result.getValue();
            } else {
                throw new TransactionException("No object exists by the key: " + key);
            }
        }
    }

    // TODO HUWEI
    public synchronized byte[] read(String table, String column, List<String> columns) throws TransactionException {
        assertState();
        
        Map<String, Result> reads = doRead(table, column, columns);
        
        for (Entry<String, Result> e : reads.entrySet()) {
        	String key = e.getValue().getKey();
        	if (readSet.containsKey(key)) {
                Result result = readSet.get(key);
                if (result.isDeleted()) {
                    throw new TransactionException("No object exists by the key: " + key);
                }
                if (result.getVersion() == 0) {
                    throw new TransactionException("No object exists by the key: " + key);
                }
                return result.getValue();
            } else {
                Result result = e.getValue();
                if (result != null) {
                    result.setDeleted(isDeleted(result.getValue()));
                    readSet.put(result.getKey(), result);

                    if (result.isDeleted()) {
                        throw new TransactionException("No object exists by the key: " + key);
                    }

                    if (result.getVersion() == 0) {
                        throw new TransactionException("No object exists by the key: " + key);
                    }
                    return result.getValue();
                } else {
                    throw new TransactionException("No object exists by the key: " + key);
                }
            }
        }
        throw new TransactionException("No object exists by the constraints...");
    }
    
    // TODO HUWEI
    public synchronized byte[] read(String table, String key_prefix,
 			List<String> columns, String constraintColumn, String constraintValue,
 			String orderColumn, boolean isAssending) throws TransactionException {
        assertState();

		List<Map<String, Result>> reads = this.doRead(table, key_prefix, columns, constraintColumn,
				constraintValue, orderColumn, isAssending);
		for (Map<String, Result> m : reads) {
			for (Entry<String, Result> e : m.entrySet()) {
				String key = e.getValue().getKey();
				
				if (readSet.containsKey(key)) {
		            Result result = readSet.get(key);
		            if (result.isDeleted()) {
		                throw new TransactionException("No object exists by the key: " + key);
		            }
		            if (result.getVersion() == 0) {
		                throw new TransactionException("No object exists by the key: " + key);
		            }
		            return result.getValue();
		        } else {
		            Result result = e.getValue();
		            if (result != null) {
		                result.setDeleted(isDeleted(result.getValue()));
		                readSet.put(result.getKey(), result);

		                if (result.isDeleted()) {
		                    throw new TransactionException("No object exists by the key: " + key);
		                }

		                if (result.getVersion() == 0) {
		                    throw new TransactionException("No object exists by the key: " + key);
		                }
		                return result.getValue();
		            } else {
		                throw new TransactionException("No object exists by the key: " + key);
		            }
		        }
			}
		}
        throw new TransactionException("No object exists by the constraints...");
    }
    
	// TODO HUWEI
	public synchronized byte[] read(String table, String key_prefix,
			String projectionColumn, String constraintColumn, int lowerBound,
			int upperBound) throws TransactionException {
		assertState();

		List<Result> x = this.doRead(table, key_prefix, projectionColumn, constraintColumn, lowerBound, upperBound);
        
		for (Result r : x) {
			String key = r.getKey();
			
			if (readSet.containsKey(key)) {
	            Result result = readSet.get(key);
	            if (result.isDeleted()) {
	                throw new TransactionException("No object exists by the key: " + key);
	            }
	            if (result.getVersion() == 0) {
	                throw new TransactionException("No object exists by the key: " + key);
	            }
	            return result.getValue();
	        } else {
	            Result result = r;
	            result.setDeleted(isDeleted(result.getValue()));
				readSet.put(result.getKey(), result);

				if (result.isDeleted()) {
				    throw new TransactionException("No object exists by the key: " + key);
				}

				if (result.getVersion() == 0) {
				    throw new TransactionException("No object exists by the key: " + key);
				}
				return result.getValue();
	        }
		}
		throw new TransactionException("No object exists by the constraints...");
    }
        
    private boolean isDeleted(byte[] data) {
        return data.length == Database.DELETE_VALUE.length &&
                Arrays.equals(data, Database.DELETE_VALUE);
    }

    public synchronized void delete(String key) throws TransactionException {
        assertState();

        Option option;
        Result result = readSet.get(key);
        if (result != null) {
            // We have already read this object.
            // Update the value in the read-set so future reads can see this write.
            if (result.isDeleted()) {
                throw new TransactionException("Object already deleted: " + key);
            }
            result.setDeleted(true);
            option = new Option(key, Database.DELETE_VALUE,
                    result.getVersion(), result.isClassic());
        } else {
            result = doRead(key);
            if (result == null) {
                // Object doesn't exist in the DB - Error!
                throw new TransactionException("Unable to delete non existing object: " + key);
            } else {
                // Object exists in the DB.
                // Update the value and add to the read-set so future reads can
                // see this write.
                result.setDeleted(true);
                option = new Option(key, Database.DELETE_VALUE,
                        result.getVersion(), result.isClassic());
                readSet.put(result.getKey(), result);
            }
        }
        writeSet.put(key, option);
    }

    public synchronized void write(String key, byte[] data) throws TransactionException {
        assertState();

        Option option;
        Result result = readSet.get(key);
        if (result != null) {
            option = new Option(key, data, result.getVersion(), result.isClassic());
            result.setValue(data);
        } else {
            result = doRead(key);
            if (result == null) {
                result = new Result(key, data, (long) 0, false);
            } else {
                result.setValue(data);
            }
            option = new Option(key, data, result.getVersion(), result.isClassic());
            readSet.put(result.getKey(), result);
        }
        writeSet.put(key, option);
    }

    public synchronized void commit() throws TransactionException {
        assertState();
        try {
            if (writeSet.size() > 0) {
                doCommit(transactionId, writeSet.values());
            }
        } finally {
            this.complete = true;
        }
    }

    private void assertState() throws TransactionException {
        if (this.transactionId == null) {
            throw new TransactionException("Read operation invoked before begin");
        } else if (this.complete) {
            throw new TransactionException("Attempted operation on completed transaction");
        }
    }

    protected abstract Result doRead(String key);

    // HUWEI
    protected abstract Map<String, Result> doRead(String table, String column, List<String> columns);

    protected abstract List<Map<String, Result>> doRead(String table, String key_prefix,
 			List<String> columns, String constraintColumn, String constraintValue,
 			String orderColumn, boolean isAssending);

    protected abstract List<Result> doRead(String table, String key_prefix,
 			String projectionColumn, String constraintColumn, int lowerBound, int upperBound);
 	
	protected abstract Integer doRead(String table, String key_prefix,
			String constraintColumn, int lowerBound, int upperBound);
 	
    protected abstract void doCommit(String transactionId,
                                     Collection<Option> options) throws TransactionException;

}
