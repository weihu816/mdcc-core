package edu.ucsb.cs.mdcc.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDatabase implements Database {

	public static String WAREHOUSE 	= 	"warehouse";
	public static String DISTRICT 	= 	"district";
	public static String CUSTOMER 	= 	"customer";
	public static String HISTORY 	= 	"history";
	public static String ORDER 		= 	"order";
	public static String NEWORDER 	= 	"new_order";
	public static String ORDERLINE 	= 	"order_line";
	public static String ITEM 		= 	"item";
	public static String STOCK 		= 	"stock";
	public static String TEST 		= 	"test";
	
	private String[] columnFamilies = { WAREHOUSE, DISTRICT, CUSTOMER, HISTORY,
			ORDER, NEWORDER, ORDERLINE, ITEM, STOCK, TEST };
	
	private Map<String, ConcurrentHashMap<String, ConcurrentHashMap<String, Record>>> db = new ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, Record>>>();
    private Map<String,TransactionRecord> transactions = new ConcurrentHashMap<String, TransactionRecord>();

    public Record get(String key) {
    	int i = key.indexOf(":");
    	int j = key.lastIndexOf(":");
    	String table = key.substring(0, i);
    	String name = key.substring(i+1, j);
    	String column = key.substring(j+1);
    	
    	Record record;
        ConcurrentHashMap<String, Record> records = db.get(table).get(column);
        if (records == null) {
        	records = new ConcurrentHashMap<String, Record>();
        	db.get(table).put(column, records);
        	record = new Record(key);
        } else {
        	record = records.get(name);
            if (record == null) {
            	record = new Record(key);
            }
        }
        return record;
    }

    public void init() {
    	for (int i = 0; i < columnFamilies.length; i++) {
    		db.put(columnFamilies[i], new ConcurrentHashMap<String, ConcurrentHashMap<String, Record>>());
    	}
    	//(new Loader(this)).load();
    }

    public void shutdown() {

    }

    public Collection<Record> getAll() {
        Collection<Record> c = new ArrayList<Record>();
    	for(int i = 0; i < columnFamilies.length; i++) {
			ConcurrentHashMap<String, ConcurrentHashMap<String, Record>> columnFamily = db.get(columnFamilies[i]);
			for(ConcurrentHashMap<String, Record> entry : columnFamily.values()) {
				c.addAll(entry.values());
			}
    	}
        return c;
    }

    public void put(Record record) {
    	String key = record.getKey();
    	int i = key.indexOf(":");
    	int j = key.lastIndexOf(":");
    	String table = key.substring(0, i);
    	String column = key.substring(j+1);
    	String name = key.substring(i+1, j);
    	
    	ConcurrentHashMap<String, Record> records = db.get(table).get(column);
    	if (records == null) {
    		records = new ConcurrentHashMap<String, Record>();
    		records.put(name, record);
    		db.get(table).put(column, records);
    	} else {
    		records.put(name, record);
    	}
    	
    }

    public TransactionRecord getTransactionRecord(String transactionId) {
        TransactionRecord record = transactions.get(transactionId);
        if (record == null) {
            record = new TransactionRecord(transactionId);
        }
        return record;
    }

    public void putTransactionRecord(TransactionRecord record) {
        transactions.put(record.getTransactionId(), record);
    }

    public void weakPut(Record record) {
        put(record);
    }

    public void weakPutTransactionRecord(TransactionRecord record) {
        transactions.put(record.getTransactionId(), record);
    }
    
    //------------------------------
    public Map<String, Record> read(String table, String column, List<String> readFields) {

		ConcurrentHashMap<String, Record> records = db.get(table).get(column);
		ConcurrentHashMap<String, Record> ret = new ConcurrentHashMap<String, Record>();
		
        if (records == null) {
            records = new ConcurrentHashMap<String, Record>();
            db.get(table).put(column, records);
        } else {
        	for (String field : readFields) {
        		Record r = records.get(field);
        		if (r != null) {
        			ret.put(field, r);
        		}
        	}
        }
        return ret;
	}
    
	public List<Map<String, Record>> read( String table, String key_prefix,
			List<String> columns, String constraintColumn,
			String constraintValue, String orderColumn, boolean isAssending) {

		List<Map<String, Record>> ret = new ArrayList<Map<String, Record>>();
		List<String> keyset = new ArrayList<String>();
		for (String t : db.get(table).keySet()) {
			if (t.indexOf(key_prefix) == 0) {
				keyset.add(t);
			}
		}
		for (String key : keyset) {
			ConcurrentHashMap<String, Record> r = db.get(table).get(key);
			if (constraintColumn == null || constraintColumn.equals("")) {
				Map<String, Record> map = new ConcurrentHashMap<String, Record>();
				for (String name : columns) {
					map.put(name, r.get(name));
				}
				ret.add(map);
			} else {
				if (constraintValue.equals(new String(r.get(constraintColumn).getValue()))) {
					Map<String, Record> map = new ConcurrentHashMap<String, Record>();
					for (String name : columns) {
						map.put(name, r.get(name));
					}
					ret.add(map);
				}
			}
		}
		return ret;
	}
	
	public List<Record> read(String table, String key_prefix,
			String projectionColumn, String constraintColumn, long lowerBound,
			long upperBound) {
		
		List<Record> ret = new ArrayList<Record>();
		
		List<String> keyset = new ArrayList<String>();
		for (String t : db.get(table).keySet()) {
			if (t.indexOf(key_prefix) == 0) {
				keyset.add(t);
			}
		}
		if (table.equals("order_line")) {
			Collections.sort(keyset, new Comparator<String>() {
				@Override
				public int compare(String o1, String o2) {
					return o2.compareTo(o1);
				}	
			});
			Iterator<String> iter = keyset.iterator();
			for (int i = 0; i < 20 && iter.hasNext(); i++) {
				ConcurrentHashMap<String, Record> r = db.get(table).get(iter.next());
				int constraintValue = Integer.valueOf(new String(r.get(constraintColumn).getValue()));
				if (constraintValue >= lowerBound && constraintValue < upperBound) {
					ret.add(r.get(projectionColumn));
				}
			}
		}
		return ret;
	}
	
	public Integer read(String table, String key_prefix, String constraintColumn, long lowerBound,
			long upperBound) {
		Integer ret = 0;
		
		List<String> keyset = new ArrayList<String>();
		for (String t : db.get(table).keySet()) {
			if (t.indexOf(key_prefix) == 0) {
				keyset.add(t);
			}
		}
		
		for (String key : keyset) {
			ConcurrentHashMap<String, Record> r = db.get(table).get(key);
			int constraintValue = Integer.valueOf(new String(r.get(constraintColumn).getValue()));
			if (constraintValue >= lowerBound && constraintValue < upperBound) {
				ret++;
			}
		}
		
		return ret;
	}
	
	public void insert(String table, String key, String[] columns, Record[] values) {
		int size = columns.length;;

		ConcurrentHashMap<String, Record> r = new ConcurrentHashMap<String, Record>();
		for (int i = 0; i < size; i++) {
			r.put(columns[i], values[i]);
		}
		db.get(table).put(key, r);

	}
}
