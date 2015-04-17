/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.xsort;

import com.google.common.base.Stopwatch;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.TopN.PriorityQueue;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.record.ExpandableHyperContainer;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

import javax.inject.Named;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

public abstract class SplaySortTemplate implements SplaySorter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SplaySortTemplate.class);

//  private SelectionVector4 values;
//  private SelectionVector4 leftPointers;
//  private SelectionVector4 rightPointers;
  private SelectionVector4 finalSv4;//This is for final sorted output
  private ExpandableHyperContainer hyperBatch;
  private FragmentContext context;
  private BufferAllocator allocator;
  private int batchCount = 0;
  private boolean hasSv2;
  private int totalCount = 0;
  private SplayBST<MyKey,Void> tree = new SplayBST<>();
  private int compares;

  @Override
  public void init(FragmentContext context, BufferAllocator allocator,  boolean hasSv2) throws SchemaChangeException {
    this.context = context;
    this.allocator = allocator;
    BufferAllocator.PreAllocator preAlloc = allocator.getNewPreAllocator();
//    preAlloc.preAllocate(4 * (limit + 1));
//    values = new SelectionVector4(preAlloc.getAllocation(), limit, Character.MAX_VALUE);
    this.hasSv2 = hasSv2;
  }

  @Override
  public void add(FragmentContext context, RecordBatchData batch) throws SchemaChangeException{
    Stopwatch watch = new Stopwatch();
    watch.start();
    if (hyperBatch == null) {
      hyperBatch = new ExpandableHyperContainer(batch.getContainer());
    } else {
      hyperBatch.addBatch(batch.getContainer());
    }

    doSetup(context, hyperBatch, null); // may not need to do this every time

    SelectionVector2 sv2 = null;
    if (hasSv2) {
      sv2 = batch.getSv2();
    }
    int count = 0;
    for (; count < batch.getRecordCount(); count++) {
      int index = (batchCount << 16) | ((hasSv2 ? sv2.getIndex(count) : count) & 65535);
      totalCount++;
      MyKey key = new MyKey(index);
      tree.put(key,null);
    }
    batchCount++;
    if (hasSv2) {
      sv2.clear();
    }
    logger.debug("Took {} us to add {} records", watch.elapsed(TimeUnit.MICROSECONDS), count);
  }

  @Override
  public void generate() {
    try {
      finalSv4 = new SelectionVector4(allocator.buffer(totalCount* 4), totalCount, 4000);
    } catch (SchemaChangeException e) {
      //won't happen
    }
    int i = 0;
    for (MyKey key : tree) {
      finalSv4.set(i++, key.index);
    }
  }

  @Override
  public VectorContainer getHyperBatch() {
    return hyperBatch;
  }

  @Override
  public SelectionVector4 getFinalSv4() {
    return finalSv4;
  }

  @Override
  public void cleanup() {
//    values.clear();
    finalSv4.clear();
    hyperBatch.clear();
  }

  private void expandVectors() {
  }

  private class MyKey implements Comparable<MyKey> {

    int index;

    MyKey(int index) {
      this.index = index;
    }

    @Override
    public int compareTo(MyKey that) {
      compares++;
      int comp = doEval(this.index, that.index);
      return comp == 0 ? 1 : comp;
    }
  }

//  public int compare(int leftIndex, int rightIndex) {
//    int sv1 = values.get(leftIndex);
//    int sv2 = values.get(rightIndex);
//    return doEval(sv1, sv2);
//  }

  public abstract void doSetup(@Named("context") FragmentContext context, @Named("incoming") VectorContainer incoming, @Named("outgoing") RecordBatch outgoing);
  public abstract int doEval(@Named("leftIndex") int leftIndex, @Named("rightIndex") int rightIndex);

  /*************************************************************************
   *  Splay tree. Supports splay-insert, -search, and -delete.
   *  Splays on every operation, regardless of the presence of the associated
   *  key prior to that operation.
   *
   *  Written by Josh Israel.
   *
   *************************************************************************/


  public class SplayBST<Key extends Comparable<Key>, Value> implements Iterable<Key>  {

    private Node root;   // root of the BST

    @Override
    public Iterator<Key> iterator() {
      return new SplayIterator();
    }

    private class SplayIterator implements Iterator<Key> {

      private Node next;

      SplayIterator() {
        next = root;
        if (next == null) {
          return;
        }
        while (next.left != null) {
          if (next.left.parent == null) {
            next.left.parent = next;
          }
          next = next.left;
        }
      }

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public Key next() {
        if (!hasNext()) throw new NoSuchElementException();
        Node r = next;
        // if you can walk right, walk right, then fully left.
        // otherwise, walk up until you come from left.
        if (next.right != null) {
          if (next.right.parent == null) {
            next.right.parent = next;
          }
          next = next.right;
          while (next.left != null) {
            if (next.left.parent == null) {
              next.left.parent = next;
            }
            next = next.left;
          }
          return r.key;
        } else {
          while(true) {
            if(next.parent == null) {
              next = null;
              return r.key;
            }
            if(next.parent.left == next){
              next = next.parent;
              return r.key;
            }
            next = next.parent;
          }
        }
      }

      @Override
      public void remove() {

      }
    }

    private Node findLowest() {
      Node node = root;
      while(node.left != null) {
        node = node.left;
      }
      return node;
    }

    // BST helper node data type
    private class Node {
      private Key key;            // key
      private Value value;        // associated data
      private Node left, right, parent;   // left and right subtrees

      public Node(Key key, Value value) {
        this.key   = key;
        this.value = value;
      }
    }

    public boolean contains(Key key) {
      return (get(key) != null);
    }

    // return value associated with the given key
    // if no such value, return null
    public Value get(Key key) {
      root = splay(root, key);
      int cmp = key.compareTo(root.key);
      if (cmp == 0) return root.value;
      else          return null;
    }

    /*************************************************************************
     *  splay insertion
     *************************************************************************/
    public void put(Key key, Value value) {
      // splay key to root
      if (root == null) {
        root = new Node(key, value);
        return;
      }

      root = splay(root, key);

      int cmp = key.compareTo(root.key);

      // Insert new node at root
      if (cmp < 0) {
        Node n = new Node(key, value);
        n.left = root.left;
        n.right = root;
        root.left = null;
        root = n;
      }

      // Insert new node at root
      else if (cmp > 0) {
        Node n = new Node(key, value);
        n.right = root.right;
        n.left = root;
        root.right = null;
        root = n;
      }

      // It was a duplicate key. Simply replace the value
      else if (cmp == 0) {
        root.value = value;
      }
    }


    /************************************************************************
     * splay function
     * **********************************************************************/
    // splay key in the tree rooted at Node h. If a node with that key exists,
    //   it is splayed to the root of the tree. If it does not, the last node
    //   along the search path for the key is splayed to the root.
    private Node splay(Node h, Key key) {
      if (h == null) return null;

      int cmp1 = key.compareTo(h.key);

      if (cmp1 < 0) {
        // key not in tree, so we're done
        if (h.left == null) {
          return h;
        }
        int cmp2 = key.compareTo(h.left.key);
        if (cmp2 < 0) {
          h.left.left = splay(h.left.left, key);
          h = rotateRight(h);
        }
        else if (cmp2 > 0) {
          h.left.right = splay(h.left.right, key);
          if (h.left.right != null)
            h.left = rotateLeft(h.left);
        }

        if (h.left == null) return h;
        else                return rotateRight(h);
      }

      else if (cmp1 > 0) {
        // key not in tree, so we're done
        if (h.right == null) {
          return h;
        }

        int cmp2 = key.compareTo(h.right.key);
        if (cmp2 < 0) {
          h.right.left  = splay(h.right.left, key);
          if (h.right.left != null)
            h.right = rotateRight(h.right);
        }
        else if (cmp2 > 0) {
          h.right.right = splay(h.right.right, key);
          h = rotateLeft(h);
        }

        if (h.right == null) return h;
        else                 return rotateLeft(h);
      }

      else return h;
    }


    /*************************************************************************
     *  helper functions
     *************************************************************************/

    // height of tree (1-node tree has height 0)
    public int height() { return height(root); }
    private int height(Node x) {
      if (x == null) return -1;
      return 1 + Math.max(height(x.left), height(x.right));
    }


    public int size() {
      return size(root);
    }

    private int size(Node x) {
      if (x == null) return 0;
      else return (1 + size(x.left) + size(x.right));
    }

    // right rotate
    private Node rotateRight(Node h) {
      Node x = h.left;
      h.left = x.right;
      x.right = h;
      return x;
    }

    // left rotate
    private Node rotateLeft(Node h) {
      Node x = h.right;
      h.right = x.left;
      x.left = h;
      return x;
    }
  }
}
