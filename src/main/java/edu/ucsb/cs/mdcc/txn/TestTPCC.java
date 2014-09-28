package edu.ucsb.cs.mdcc.txn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import edu.ucsb.cs.mdcc.Result;
import edu.ucsb.cs.mdcc.paxos.Transaction;
import edu.ucsb.cs.mdcc.paxos.TransactionException;


public class TestTPCC {

	public static Random random = new Random();
	
    public static void main(String[] args) throws TransactionException {
        final TransactionFactory fac = new TransactionFactory();

    	System.out.println("Step 1: Initializing accounts");
        MDCCTransaction t1 = fac.create();
        t1.begin();
		t1.write("customer:name:1_1_1", "cus1".getBytes());
		t1.write("customer:name:1_1_2", "cus2".getBytes());
		t1.write("customer:name:1_1_3", "cus3".getBytes());
		t1.write("customer:addr:1_1_1", "addr1".getBytes());
		t1.write("customer:addr:1_1_2", "addr2".getBytes());
		t1.write("customer:addr:1_1_3", "addr3".getBytes());
		t1.write("customer:age:1_1_1", "10".getBytes());
		t1.write("customer:age:1_1_2", "20".getBytes());
		t1.write("customer:age:1_1_3", "30".getBytes());
        List<String> columns = new ArrayList<String>();
        columns.add("name");
        columns.add("addr");
        columns.add("age");
        Map<String, Result> x = t1.read("customer", "1_1_2", columns);
        int y = t1.read("warehouse", "1_1", "age", 15, 25);
        System.out.println("y = " + y);
        System.out.println(new String(x.get("name").getValue()));
        System.out.println(new String(x.get("addr").getValue()));
        System.out.println(new String(x.get("age").getValue()));
		t1.commit();
        System.out.println("Transaction 1 completed...");
        System.out.println("==========================================\n");
		
    	
        
    }

}
