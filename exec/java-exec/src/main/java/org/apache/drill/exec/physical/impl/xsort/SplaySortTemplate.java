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
  private static final int INITIAL_SIZE = 32*1024;
  private static final int BATCH_SIZE = 4096;

  private SelectionVector4 values;
  private SelectionVector4 leftPointers;
  private SelectionVector4 rightPointers;
  private SelectionVector4 parentPointers;
  private SelectionVector4 finalSv4;//This is for final sorted output
  private ExpandableHyperContainer hyperBatch;
  private FragmentContext context;
  private BufferAllocator allocator;
  private int batchCount = 0;
  private boolean hasSv2;
  private int totalCount = 0;
  private SplayBST tree = new SplayBST();
  private int compares;

  private static final int NULL = -1;
  private int currentSize;

  @Override
  public void init(FragmentContext context, BufferAllocator allocator,  boolean hasSv2) throws SchemaChangeException {
    this.context = context;
    this.allocator = allocator;
    this.values = new SelectionVector4(allocator.buffer(INITIAL_SIZE * 4), INITIAL_SIZE, BATCH_SIZE);
    for (int i = 0; i < INITIAL_SIZE; i++) {
      values.set(i, NULL);
    }
    this.leftPointers = new SelectionVector4(allocator.buffer(INITIAL_SIZE * 4), INITIAL_SIZE, BATCH_SIZE);
    for (int i = 0; i < INITIAL_SIZE; i++) {
      leftPointers.set(i, NULL);
    }
    this.rightPointers = new SelectionVector4(allocator.buffer(INITIAL_SIZE * 4), INITIAL_SIZE, BATCH_SIZE);
    for (int i = 0; i < INITIAL_SIZE; i++) {
      rightPointers.set(i, NULL);
    }
    this.parentPointers = new SelectionVector4(allocator.buffer(INITIAL_SIZE * 4), INITIAL_SIZE, BATCH_SIZE);
    for (int i = 0; i < INITIAL_SIZE; i++) {
      parentPointers.set(i, NULL);
    }
    this.hasSv2 = hasSv2;
    currentSize = INITIAL_SIZE;
  }

  @Override
  public void add(FragmentContext context, RecordBatchData batch) throws SchemaChangeException{
    if (totalCount + batch.getRecordCount() > currentSize) {
      expandVectors();
    }
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
      values.set(totalCount, index);
      tree.put(totalCount);
      totalCount++;
    }
    batchCount++;
    if (hasSv2) {
      sv2.clear();
    }
    logger.debug("Took {} us to add {} records", watch.elapsed(TimeUnit.MICROSECONDS), count);
  }

  private void print() {
    StringBuilder b = new StringBuilder();
    b.append("root: " + tree.root + "\n");
    for (int i = 0; i < totalCount; i++) {
      b.append(values.get(i) + "\t");
      b.append(leftPointers.get(i) + "\t");
      b.append(rightPointers.get(i) + "\t");
      b.append("\n");
    }
    System.out.println(b);
  }

  @Override
  public void generate() {
    Stopwatch watch = new Stopwatch();
    watch.start();
    try {
      finalSv4 = new SelectionVector4(allocator.buffer(totalCount* 4), totalCount, 4000);
    } catch (SchemaChangeException e) {
      //won't happen
    }
    int i = 0;
    for (int key : tree) {
      finalSv4.set(i++, values.get(key));
    }
    values.clear();
    leftPointers.clear();
    rightPointers.clear();
    parentPointers.clear();
    logger.debug("Took {} us to generate final order", watch.elapsed(TimeUnit.MICROSECONDS));
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
    logger.debug("{} compares", compares);
//    values.clear();
    finalSv4.clear();
    hyperBatch.clear();
  }

  private void expandVectors() {
    SelectionVector4 newVector;
    try {
      newVector = new SelectionVector4(allocator.buffer(currentSize * 2 * 4), currentSize, BATCH_SIZE);
      for (int i = 0; i < currentSize; i++) {
        newVector.set(i, values.get(i));
      }
      for (int i = currentSize; i < currentSize * 2; i++) {
        newVector.set(i, NULL);
      }
      values.clear();
      values = newVector;

      newVector = new SelectionVector4(allocator.buffer(currentSize * 2 * 4), currentSize, BATCH_SIZE);
      for (int i = 0; i < currentSize; i++) {
        newVector.set(i, leftPointers.get(i));
      }
      for (int i = currentSize; i < currentSize * 2; i++) {
        newVector.set(i, NULL);
      }
      leftPointers.clear();
      leftPointers = newVector;

      newVector = new SelectionVector4(allocator.buffer(currentSize * 2 * 4), currentSize, BATCH_SIZE);
      for (int i = 0; i < currentSize; i++) {
        newVector.set(i, rightPointers.get(i));
      }
      for (int i = currentSize; i < currentSize * 2; i++) {
        newVector.set(i, NULL);
      }
      rightPointers.clear();
      rightPointers = newVector;

      newVector = new SelectionVector4(allocator.buffer(currentSize * 2 * 4), currentSize, BATCH_SIZE);
      for (int i = 0; i < currentSize; i++) {
        newVector.set(i, parentPointers.get(i));
      }
      for (int i = currentSize; i < currentSize * 2; i++) {
        newVector.set(i, NULL);
      }
      parentPointers.clear();
      parentPointers = newVector;
      currentSize *= 2;
    } catch (SchemaChangeException e) {
      // no op; won't happen
    }
  }

  public int compare(int leftIndex, int rightIndex) {
    compares++;
    int sv1 = values.get(leftIndex);
    int sv2 = values.get(rightIndex);
    int comp = doEval(sv1, sv2);
    return comp == 0 ? 1 : comp;
  }

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


  public class SplayBST implements Iterable<Integer>  {

    private int root = NULL;   // root of the BST

    @Override
    public Iterator<Integer> iterator() {
      return new SplayIterator();
    }

    private class SplayIterator implements Iterator<Integer> {

      private int next;

      SplayIterator() {
        next = root;
        if (next == NULL) {
          return;
        }
        while (getLeft(next) != NULL) {
          if (getParent(getLeft(next)) == NULL) {
            setParent(getLeft(next), next);
          }
          next = getLeft(next);
        }
      }

      @Override
      public boolean hasNext() {
        return next != NULL;
      }

      @Override
      public Integer next () {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        int r = next;
        // if you can walk right, walk right, then fully left.
        // otherwise, walk up until you come from left.
        if (getRight(next) != NULL) {
          if (getParent(getRight(next)) == NULL) {
            setParent(getRight(next), next);
          }
          next = getRight(next);
          while (getLeft(next) != NULL) {
            if (getParent(getLeft(next)) == NULL) {
              setParent(getLeft(next), next);
            }
            next = getLeft(next);
          }
          return r;
        } else {
          while (true) {
            if (getParent(next) == NULL) {
              next = NULL;
              return r;
            }
            if (getLeft(getParent(next)) == next) {
              next = getParent(next);
              return r;
            }
            next = getParent(next);
          }
        }
      }

      @Override
      public void remove() {

      }
    }

    /*
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

     */

      /**
       * **********************************************************************
       * splay insertion
       * ***********************************************************************
       */
      public void put(int key) {
        // splay key to root
        if (root == NULL) {
          root = key;
          return;
        }

        root = splay(root, key);

        int cmp = compare(key, root);

        // Insert new node at root
        if (cmp < 0) {
          int n = key;
          setLeft(n, getLeft(root));
          setRight(n, root);
          setLeft(root, NULL);
          root = n;
        }

        // Insert new node at root
        else if (cmp > 0) {
          int n = key;
          setRight(n, getRight(root));
          setLeft(n, root);
          setRight(root, NULL);
          root = n;
        }

        // It was a duplicate key. Simply replace the value
        else if (cmp == 0) {
          root = root;
        }
      }

      /**
       * *********************************************************************
       * splay function
       * *********************************************************************
       */
      // splay key in the tree rooted at Node h. If a node with that key exists,
      //   it is splayed to the root of the tree. If it does not, the last node
      //   along the search path for the key is splayed to the root.
      private int splay(int h, int key) {
        if (h == NULL) {
          return NULL;
        }

        int cmp1 = compare(key, h);

        if (cmp1 < 0) {
          // key not in tree, so we're done
          if (getLeft(h) == NULL) {
            return h;
          }
          int cmp2 = compare(key, getLeft(h));
          if (cmp2 < 0) {
            setLeft(getLeft(h), splay(getLeft(getLeft(h)), key));
            h = rotateRight(h);
          } else if (cmp2 > 0) {
            setRight(getLeft(h), splay(getRight(getLeft(h)), key));
            if (getRight(getLeft(h)) != NULL) {
              setLeft(h, rotateLeft(getLeft(h)));
            }
          }

          if (getLeft(h) == NULL) {
            return h;
          }
          else {
            return rotateRight(h);
          }
        } else if (cmp1 > 0) {
          // key not in tree, so we're done
          if (getRight(h) == NULL) {
            return h;
          }

          int cmp2 = compare(key, getRight(h));
          if (cmp2 < 0) {
            setLeft(getRight(h), splay(getLeft(getRight(h)), key));
            if (getLeft(getRight(h)) != NULL) {
              setRight(h, rotateRight(getRight(h)));
            }
          } else if (cmp2 > 0) {
            setRight(getRight(h), splay(getRight(getRight(h)), key));
            h = rotateLeft(h);
          }

          if (getRight(h) == NULL) {
            return h;
          } else {
            return rotateLeft(h);
          }
        } else {
          return h;
        }
      }


    /*************************************************************************
     *  helper functions
     *************************************************************************/

    private int getLeft(int key) {
      return leftPointers.get(key);
    }

    private void setLeft(int key, int value) {
      leftPointers.set(key, value);
    }

    private int getRight(int key) {
      return rightPointers.get(key);
    }

    private void setRight(int key, int value) {
      rightPointers.set(key, value);
    }

    private int getParent(int key) {
      return parentPointers.get(key);
    }

    private void setParent(int key, int value) {
      parentPointers.set(key, value);
    }

    // height of tree (1-node tree has height 0)
//    public int height() { return height(root); }
//    private int height(Node x) {
//      if (x == null) return -1;
//      return 1 + Math.max(height(x.left), height(x.right));
//    }


//    public int size() {
//      return size(root);
//    }
//
//    private int size(Node x) {
//      if (x == null) return 0;
//      else return (1 + size(x.left) + size(x.right));
//    }

    // right rotate
    private int rotateRight(int h) {
      int x = getLeft(h);
      setLeft(h, getRight(x));
      setRight(x, h);
      return x;
    }

    // left rotate
    private int rotateLeft(int h) {
      int x = getRight(h);
      setRight(h, getLeft(x));
      setLeft(x, h);
      return x;
    }
  }
}
