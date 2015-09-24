package org.apache.drill;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by stevenphillips on 9/24/15.
 */
public class TpchGen {

  public static void main(String[] args) throws IOException {
    fileOutputStream = new FileOutputStream("/tmp/lineorders6.json");
    bufferedOutputStream = new BufferedOutputStream(fileOutputStream, 8192*8);
    String customerFile = "/drill/customer.tbl";
    String orderFile = "/drill/orders.tbl";
    String lineitemFile = "/drill/lineitem.tbl";
    BufferedReader reader = new BufferedReader(new FileReader(customerFile));

    Map<Integer, Customer> customerMap = new HashMap<>();

    String line;
    while ((line = reader.readLine()) != null) {
      String[] tokens = line.split("\\|");
      int custkey = Integer.parseInt(tokens[0]);
      Customer c = new Customer(tokens);
      customerMap.put(custkey, c);
    }

    reader = new BufferedReader(new FileReader(orderFile));

    Map<Integer, Order> orderMap = new HashMap<>();

    while ((line = reader.readLine()) != null) {
      String[] tokens = line.split("\\|");
      Order o = new Order(tokens, customerMap);
      orderMap.put(o.o_orderkey, o);
    }

    customerMap.clear();

    reader = new BufferedReader(new FileReader(lineitemFile));

    Integer previousOrderKey = null;
    while ((line = reader.readLine()) != null) {
      String[] tokens = line.split("\\|");
      Integer orderkey = Integer.parseInt(tokens[0]);
      if (orderkey < 5000001) continue;
      Lineitem l = new Lineitem(tokens);
      if (!orderkey.equals(previousOrderKey)) {
        Order o = orderMap.get(previousOrderKey);
        if (o != null) {
          print(o);
          orderMap.remove(o);
        }
        previousOrderKey = orderkey;
      }
      if (orderkey > 6000000) break;
//      if (orderkey > 100) break;
      Order o = orderMap.get(orderkey);
      o.addLineitem(l);
    }

    bufferedOutputStream.close();
    fileOutputStream.close();
  }

  private static ObjectMapper mapper = new ObjectMapper();
  private static FileOutputStream fileOutputStream;
  private static BufferedOutputStream bufferedOutputStream;

  private static void print(Object obj) throws IOException {
    OutputStream os = new OutputStream() {
      @Override
      public void write(int b) throws IOException {
        bufferedOutputStream.write(b);
      }
    };

    mapper.writer().withDefaultPrettyPrinter().writeValue(os, obj);
  }

  public static class Lineitem {
    public int l_partkey;
    public int l_suppkey;
    public int l_linenumber;
    public int l_quantity;
    public float l_extendedprice;
    public float l_discount;
    public float l_tax;
    public String l_returnflag;
    public String l_linestatus;
    public String l_shipdate;
    public String l_commitdate;
    public String l_receiptdate;
    public String l_shipinstruct;
    public String l_shipmode;
    public String l_comment;

    public Lineitem(String[] args) {
      l_partkey = Integer.parseInt(args[1]);
      l_suppkey = Integer.parseInt(args[2]);
      l_linenumber = Integer.parseInt(args[3]);
      l_quantity = Integer.parseInt(args[4]);
      l_extendedprice = Float.parseFloat(args[5]);
      l_discount = Float.parseFloat(args[6]);
      l_tax = Float.parseFloat(args[7]);
      l_returnflag = args[8];
      l_linestatus = args[9];
      l_shipdate = args[10];
      l_commitdate = args[11];
      l_receiptdate = args[12];
      l_shipinstruct = args[13];
      l_shipmode = args[14];
      l_comment = args[15];
    }
  }

  public static class Order {
    public int o_orderkey;
    public List<Lineitem> o_lineitems;
    public Customer o_customer;
    public String o_orderstatus;
    public float o_totalprice;
    public String o_orderdate;;
    public String o_orderpriority;
    public String o_clerk;
    public int o_shippriority;
    public String o_comment;

    public Order(String[] args, Map<Integer,Customer> customerMap) {
      o_orderkey = Integer.parseInt(args[0]);
      o_customer = customerMap.get(Integer.parseInt(args[1]));
      o_orderstatus = args[2];
      o_totalprice = Float.parseFloat(args[3]);
      o_orderdate = args[4];
      o_orderpriority = args[5];
      o_clerk = args[6];
      o_shippriority = Integer.parseInt(args[7]);
      o_comment = args[8];
      o_lineitems = Lists.newArrayList();
    }

    public void addLineitem(Lineitem l) {
      o_lineitems.add(l);
    }
  }

  public static class Customer {
    public int c_custkey;
    public String c_name;
    public String c_address;
    public int c_nationkey;
    public String c_phone;
    public float c_acctbal;
    public String c_mktsegment;
    public String c_comment;

    public Customer(String[] args) {
      c_custkey = Integer.parseInt(args[0]);
      c_name = args[1];
      c_address = args[2];
      c_nationkey = Integer.parseInt(args[3]);
      c_phone = args[4];
      c_acctbal = Float.parseFloat(args[5]);
      c_mktsegment = args[6];
      c_comment = args[7];
    }
  }
}
