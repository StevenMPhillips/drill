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
package org.apache.arrow.memory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ArrowBuf.TransferResult;

import org.apache.arrow.memory.OutOfMemoryException;
import org.junit.Ignore;
import org.junit.Test;

public class TestBaseAllocator {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestBaseAllocator.class);

  private final static int MAX_ALLOCATION = 8 * 1024;

/*
  // ---------------------------------------- DEBUG -----------------------------------

  @After
  public void checkBuffers() {
    final int bufferCount = UnsafeDirectLittleEndian.getBufferCount();
    if (bufferCount != 0) {
      UnsafeDirectLittleEndian.logBuffers(logger);
      UnsafeDirectLittleEndian.releaseBuffers();
    }

    assertEquals(0, bufferCount);
  }

//  @AfterClass
//  public static void dumpBuffers() {
//    UnsafeDirectLittleEndian.logBuffers(logger);
//  }

  // ---------------------------------------- DEBUG ------------------------------------
*/


  @Test
  public void test_privateMax() throws Exception {
    try(final RootAllocator rootAllocator =
        new RootAllocator(MAX_ALLOCATION)) {
      final ArrowBuf ArrowBuf1 = rootAllocator.buffer(MAX_ALLOCATION / 2);
      assertNotNull("allocation failed", ArrowBuf1);

      try(final BufferAllocator childAllocator =
          rootAllocator.newChildAllocator("noLimits", 0, MAX_ALLOCATION)) {
        final ArrowBuf ArrowBuf2 = childAllocator.buffer(MAX_ALLOCATION / 2);
        assertNotNull("allocation failed", ArrowBuf2);
        ArrowBuf2.release();
      }

      ArrowBuf1.release();
    }
  }

  @Test(expected=IllegalStateException.class)
  public void testRootAllocator_closeWithOutstanding() throws Exception {
    try {
      try(final RootAllocator rootAllocator =
          new RootAllocator(MAX_ALLOCATION)) {
        final ArrowBuf ArrowBuf = rootAllocator.buffer(512);
        assertNotNull("allocation failed", ArrowBuf);
      }
    } finally {
      /*
       * We expect there to be one unreleased underlying buffer because we're closing
       * without releasing it.
       */
/*
      // ------------------------------- DEBUG ---------------------------------
      final int bufferCount = UnsafeDirectLittleEndian.getBufferCount();
      UnsafeDirectLittleEndian.releaseBuffers();
      assertEquals(1, bufferCount);
      // ------------------------------- DEBUG ---------------------------------
*/
    }
  }

  @Test
  public void testRootAllocator_getEmpty() throws Exception {
    try(final RootAllocator rootAllocator =
        new RootAllocator(MAX_ALLOCATION)) {
      final ArrowBuf ArrowBuf = rootAllocator.buffer(0);
      assertNotNull("allocation failed", ArrowBuf);
      assertEquals("capacity was non-zero", 0, ArrowBuf.capacity());
      ArrowBuf.release();
    }
  }

  @Ignore // TODO(DRILL-2740)
  @Test(expected = IllegalStateException.class)
  public void testAllocator_unreleasedEmpty() throws Exception {
    try(final RootAllocator rootAllocator =
        new RootAllocator(MAX_ALLOCATION)) {
      @SuppressWarnings("unused")
      final ArrowBuf ArrowBuf = rootAllocator.buffer(0);
    }
  }

  @Test
  public void testAllocator_transferOwnership() throws Exception {
    try(final RootAllocator rootAllocator =
        new RootAllocator(MAX_ALLOCATION)) {
      final BufferAllocator childAllocator1 =
          rootAllocator.newChildAllocator("changeOwnership1", 0, MAX_ALLOCATION);
      final BufferAllocator childAllocator2 =
          rootAllocator.newChildAllocator("changeOwnership2", 0, MAX_ALLOCATION);

      final ArrowBuf ArrowBuf1 = childAllocator1.buffer(MAX_ALLOCATION / 4);
      rootAllocator.verify();
      TransferResult transferOwnership = ArrowBuf1.transferOwnership(childAllocator2);
      final boolean allocationFit = transferOwnership.allocationFit;
      rootAllocator.verify();
      assertTrue(allocationFit);

      ArrowBuf1.release();
      childAllocator1.close();
      rootAllocator.verify();

      transferOwnership.buffer.release();
      childAllocator2.close();
    }
  }

  @Test
  public void testAllocator_shareOwnership() throws Exception {
    try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {
      final BufferAllocator childAllocator1 = rootAllocator.newChildAllocator("shareOwnership1", 0, MAX_ALLOCATION);
      final BufferAllocator childAllocator2 = rootAllocator.newChildAllocator("shareOwnership2", 0, MAX_ALLOCATION);
      final ArrowBuf ArrowBuf1 = childAllocator1.buffer(MAX_ALLOCATION / 4);
      rootAllocator.verify();

      // share ownership of buffer.
      final ArrowBuf ArrowBuf2 = ArrowBuf1.retain(childAllocator2);
      rootAllocator.verify();
      assertNotNull(ArrowBuf2);
      assertNotEquals(ArrowBuf2, ArrowBuf1);

      // release original buffer (thus transferring ownership to allocator 2. (should leave allocator 1 in empty state)
      ArrowBuf1.release();
      rootAllocator.verify();
      childAllocator1.close();
      rootAllocator.verify();

      final BufferAllocator childAllocator3 = rootAllocator.newChildAllocator("shareOwnership3", 0, MAX_ALLOCATION);
      final ArrowBuf ArrowBuf3 = ArrowBuf1.retain(childAllocator3);
      assertNotNull(ArrowBuf3);
      assertNotEquals(ArrowBuf3, ArrowBuf1);
      assertNotEquals(ArrowBuf3, ArrowBuf2);
      rootAllocator.verify();

      ArrowBuf2.release();
      rootAllocator.verify();
      childAllocator2.close();
      rootAllocator.verify();

      ArrowBuf3.release();
      rootAllocator.verify();
      childAllocator3.close();
    }
  }

  @Test
  public void testRootAllocator_createChildAndUse() throws Exception {
    try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {
      try (final BufferAllocator childAllocator = rootAllocator.newChildAllocator("createChildAndUse", 0,
          MAX_ALLOCATION)) {
        final ArrowBuf ArrowBuf = childAllocator.buffer(512);
        assertNotNull("allocation failed", ArrowBuf);
        ArrowBuf.release();
      }
    }
  }

  @Test(expected=IllegalStateException.class)
  public void testRootAllocator_createChildDontClose() throws Exception {
    try {
      try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {
        final BufferAllocator childAllocator = rootAllocator.newChildAllocator("createChildDontClose", 0,
            MAX_ALLOCATION);
        final ArrowBuf ArrowBuf = childAllocator.buffer(512);
        assertNotNull("allocation failed", ArrowBuf);
      }
    } finally {
      /*
       * We expect one underlying buffer because we closed a child allocator without
       * releasing the buffer allocated from it.
       */
/*
      // ------------------------------- DEBUG ---------------------------------
      final int bufferCount = UnsafeDirectLittleEndian.getBufferCount();
      UnsafeDirectLittleEndian.releaseBuffers();
      assertEquals(1, bufferCount);
      // ------------------------------- DEBUG ---------------------------------
*/
    }
  }

  private static void allocateAndFree(final BufferAllocator allocator) {
    final ArrowBuf ArrowBuf = allocator.buffer(512);
    assertNotNull("allocation failed", ArrowBuf);
    ArrowBuf.release();

    final ArrowBuf ArrowBuf2 = allocator.buffer(MAX_ALLOCATION);
    assertNotNull("allocation failed", ArrowBuf2);
    ArrowBuf2.release();

    final int nBufs = 8;
    final ArrowBuf[] ArrowBufs = new ArrowBuf[nBufs];
    for(int i = 0; i < ArrowBufs.length; ++i) {
      ArrowBuf ArrowBufi = allocator.buffer(MAX_ALLOCATION / nBufs);
      assertNotNull("allocation failed", ArrowBufi);
      ArrowBufs[i] = ArrowBufi;
    }
    for(ArrowBuf ArrowBufi : ArrowBufs) {
      ArrowBufi.release();
    }
  }

  @Test
  public void testAllocator_manyAllocations() throws Exception {
    try(final RootAllocator rootAllocator =
        new RootAllocator(MAX_ALLOCATION)) {
      try(final BufferAllocator childAllocator =
          rootAllocator.newChildAllocator("manyAllocations", 0, MAX_ALLOCATION)) {
        allocateAndFree(childAllocator);
      }
    }
  }

  @Test
  public void testAllocator_overAllocate() throws Exception {
    try(final RootAllocator rootAllocator =
        new RootAllocator(MAX_ALLOCATION)) {
      try(final BufferAllocator childAllocator =
          rootAllocator.newChildAllocator("overAllocate", 0, MAX_ALLOCATION)) {
        allocateAndFree(childAllocator);

        try {
          childAllocator.buffer(MAX_ALLOCATION + 1);
          fail("allocated memory beyond max allowed");
        } catch (OutOfMemoryException e) {
          // expected
        }
      }
    }
  }

  @Test
  public void testAllocator_overAllocateParent() throws Exception {
    try(final RootAllocator rootAllocator =
        new RootAllocator(MAX_ALLOCATION)) {
      try(final BufferAllocator childAllocator =
          rootAllocator.newChildAllocator("overAllocateParent", 0, MAX_ALLOCATION)) {
        final ArrowBuf ArrowBuf1 = rootAllocator.buffer(MAX_ALLOCATION / 2);
        assertNotNull("allocation failed", ArrowBuf1);
        final ArrowBuf ArrowBuf2 = childAllocator.buffer(MAX_ALLOCATION / 2);
        assertNotNull("allocation failed", ArrowBuf2);

        try {
          childAllocator.buffer(MAX_ALLOCATION / 4);
          fail("allocated memory beyond max allowed");
        } catch (OutOfMemoryException e) {
          // expected
        }

        ArrowBuf1.release();
        ArrowBuf2.release();
      }
    }
  }

  private static void testAllocator_sliceUpBufferAndRelease(
      final RootAllocator rootAllocator, final BufferAllocator bufferAllocator) {
    final ArrowBuf ArrowBuf1 = bufferAllocator.buffer(MAX_ALLOCATION / 2);
    rootAllocator.verify();

    final ArrowBuf ArrowBuf2 = ArrowBuf1.slice(16, ArrowBuf1.capacity() - 32);
    rootAllocator.verify();
    final ArrowBuf ArrowBuf3 = ArrowBuf2.slice(16, ArrowBuf2.capacity() - 32);
    rootAllocator.verify();
    @SuppressWarnings("unused")
    final ArrowBuf ArrowBuf4 = ArrowBuf3.slice(16, ArrowBuf3.capacity() - 32);
    rootAllocator.verify();

    ArrowBuf3.release(); // since they share refcounts, one is enough to release them all
    rootAllocator.verify();
  }

  @Test
  public void testAllocator_createSlices() throws Exception {
    try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {
      testAllocator_sliceUpBufferAndRelease(rootAllocator, rootAllocator);

      try (final BufferAllocator childAllocator = rootAllocator.newChildAllocator("createSlices", 0, MAX_ALLOCATION)) {
        testAllocator_sliceUpBufferAndRelease(rootAllocator, childAllocator);
      }
      rootAllocator.verify();

      testAllocator_sliceUpBufferAndRelease(rootAllocator, rootAllocator);

      try (final BufferAllocator childAllocator = rootAllocator.newChildAllocator("createSlices", 0, MAX_ALLOCATION)) {
        try (final BufferAllocator childAllocator2 =
            childAllocator.newChildAllocator("createSlices", 0, MAX_ALLOCATION)) {
          final ArrowBuf ArrowBuf1 = childAllocator2.buffer(MAX_ALLOCATION / 8);
          @SuppressWarnings("unused")
          final ArrowBuf ArrowBuf2 = ArrowBuf1.slice(MAX_ALLOCATION / 16, MAX_ALLOCATION / 16);
          testAllocator_sliceUpBufferAndRelease(rootAllocator, childAllocator);
          ArrowBuf1.release();
          rootAllocator.verify();
        }
        rootAllocator.verify();

        testAllocator_sliceUpBufferAndRelease(rootAllocator, childAllocator);
      }
      rootAllocator.verify();
    }
  }

  @Test
  public void testAllocator_sliceRanges() throws Exception {
//    final AllocatorOwner allocatorOwner = new NamedOwner("sliceRanges");
    try(final RootAllocator rootAllocator =
        new RootAllocator(MAX_ALLOCATION)) {
      // Populate a buffer with byte values corresponding to their indices.
      final ArrowBuf ArrowBuf = rootAllocator.buffer(256);
      assertEquals(256, ArrowBuf.capacity());
      assertEquals(0, ArrowBuf.readerIndex());
      assertEquals(0, ArrowBuf.readableBytes());
      assertEquals(0, ArrowBuf.writerIndex());
      assertEquals(256, ArrowBuf.writableBytes());

      final ArrowBuf slice3 = (ArrowBuf) ArrowBuf.slice();
      assertEquals(0, slice3.readerIndex());
      assertEquals(0, slice3.readableBytes());
      assertEquals(0, slice3.writerIndex());
//      assertEquals(256, slice3.capacity());
//      assertEquals(256, slice3.writableBytes());

      for(int i = 0; i < 256; ++i) {
        ArrowBuf.writeByte(i);
      }
      assertEquals(0, ArrowBuf.readerIndex());
      assertEquals(256, ArrowBuf.readableBytes());
      assertEquals(256, ArrowBuf.writerIndex());
      assertEquals(0, ArrowBuf.writableBytes());

      final ArrowBuf slice1 = (ArrowBuf) ArrowBuf.slice();
      assertEquals(0, slice1.readerIndex());
      assertEquals(256, slice1.readableBytes());
      for(int i = 0; i < 10; ++i) {
        assertEquals(i, slice1.readByte());
      }
      assertEquals(256 - 10, slice1.readableBytes());
      for(int i = 0; i < 256; ++i) {
        assertEquals((byte) i, slice1.getByte(i));
      }

      final ArrowBuf slice2 = (ArrowBuf) ArrowBuf.slice(25, 25);
      assertEquals(0, slice2.readerIndex());
      assertEquals(25, slice2.readableBytes());
      for(int i = 25; i < 50; ++i) {
        assertEquals(i, slice2.readByte());
      }

/*
      for(int i = 256; i > 0; --i) {
        slice3.writeByte(i - 1);
      }
      for(int i = 0; i < 256; ++i) {
        assertEquals(255 - i, slice1.getByte(i));
      }
*/

      ArrowBuf.release(); // all the derived buffers share this fate
    }
  }

  @Test
  public void testAllocator_slicesOfSlices() throws Exception {
//    final AllocatorOwner allocatorOwner = new NamedOwner("slicesOfSlices");
    try(final RootAllocator rootAllocator =
        new RootAllocator(MAX_ALLOCATION)) {
      // Populate a buffer with byte values corresponding to their indices.
      final ArrowBuf ArrowBuf = rootAllocator.buffer(256);
      for(int i = 0; i < 256; ++i) {
        ArrowBuf.writeByte(i);
      }

      // Slice it up.
      final ArrowBuf slice0 = ArrowBuf.slice(0, ArrowBuf.capacity());
      for(int i = 0; i < 256; ++i) {
        assertEquals((byte) i, ArrowBuf.getByte(i));
      }

      final ArrowBuf slice10 = slice0.slice(10, ArrowBuf.capacity() - 10);
      for(int i = 10; i < 256; ++i) {
        assertEquals((byte) i, slice10.getByte(i - 10));
      }

      final ArrowBuf slice20 = slice10.slice(10, ArrowBuf.capacity() - 20);
      for(int i = 20; i < 256; ++i) {
        assertEquals((byte) i, slice20.getByte(i - 20));
      }

      final ArrowBuf slice30 = slice20.slice(10,  ArrowBuf.capacity() - 30);
      for(int i = 30; i < 256; ++i) {
        assertEquals((byte) i, slice30.getByte(i - 30));
      }

      ArrowBuf.release();
    }
  }

  @Test
  public void testAllocator_transferSliced() throws Exception {
    try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {
      final BufferAllocator childAllocator1 = rootAllocator.newChildAllocator("transferSliced1", 0, MAX_ALLOCATION);
      final BufferAllocator childAllocator2 = rootAllocator.newChildAllocator("transferSliced2", 0, MAX_ALLOCATION);

      final ArrowBuf ArrowBuf1 = childAllocator1.buffer(MAX_ALLOCATION / 8);
      final ArrowBuf ArrowBuf2 = childAllocator2.buffer(MAX_ALLOCATION / 8);

      final ArrowBuf ArrowBuf1s = ArrowBuf1.slice(0, ArrowBuf1.capacity() / 2);
      final ArrowBuf ArrowBuf2s = ArrowBuf2.slice(0, ArrowBuf2.capacity() / 2);

      rootAllocator.verify();

      TransferResult result1 = ArrowBuf2s.transferOwnership(childAllocator1);
      rootAllocator.verify();
      TransferResult result2 = ArrowBuf1s.transferOwnership(childAllocator2);
      rootAllocator.verify();

      result1.buffer.release();
      result2.buffer.release();

      ArrowBuf1s.release(); // releases ArrowBuf1
      ArrowBuf2s.release(); // releases ArrowBuf2

      childAllocator1.close();
      childAllocator2.close();
    }
  }

  @Test
  public void testAllocator_shareSliced() throws Exception {
    try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {
      final BufferAllocator childAllocator1 = rootAllocator.newChildAllocator("transferSliced", 0, MAX_ALLOCATION);
      final BufferAllocator childAllocator2 = rootAllocator.newChildAllocator("transferSliced", 0, MAX_ALLOCATION);

      final ArrowBuf ArrowBuf1 = childAllocator1.buffer(MAX_ALLOCATION / 8);
      final ArrowBuf ArrowBuf2 = childAllocator2.buffer(MAX_ALLOCATION / 8);

      final ArrowBuf ArrowBuf1s = ArrowBuf1.slice(0, ArrowBuf1.capacity() / 2);
      final ArrowBuf ArrowBuf2s = ArrowBuf2.slice(0, ArrowBuf2.capacity() / 2);

      rootAllocator.verify();

      final ArrowBuf ArrowBuf2s1 = ArrowBuf2s.retain(childAllocator1);
      final ArrowBuf ArrowBuf1s2 = ArrowBuf1s.retain(childAllocator2);
      rootAllocator.verify();

      ArrowBuf1s.release(); // releases ArrowBuf1
      ArrowBuf2s.release(); // releases ArrowBuf2
      rootAllocator.verify();

      ArrowBuf2s1.release(); // releases the shared ArrowBuf2 slice
      ArrowBuf1s2.release(); // releases the shared ArrowBuf1 slice

      childAllocator1.close();
      childAllocator2.close();
    }
  }

  @Test
  public void testAllocator_transferShared() throws Exception {
    try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {
      final BufferAllocator childAllocator1 = rootAllocator.newChildAllocator("transferShared1", 0, MAX_ALLOCATION);
      final BufferAllocator childAllocator2 = rootAllocator.newChildAllocator("transferShared2", 0, MAX_ALLOCATION);
      final BufferAllocator childAllocator3 = rootAllocator.newChildAllocator("transferShared3", 0, MAX_ALLOCATION);

      final ArrowBuf ArrowBuf1 = childAllocator1.buffer(MAX_ALLOCATION / 8);

      boolean allocationFit;

      ArrowBuf ArrowBuf2 = ArrowBuf1.retain(childAllocator2);
      rootAllocator.verify();
      assertNotNull(ArrowBuf2);
      assertNotEquals(ArrowBuf2, ArrowBuf1);

      TransferResult result = ArrowBuf1.transferOwnership(childAllocator3);
      allocationFit = result.allocationFit;
      final ArrowBuf ArrowBuf3 = result.buffer;
      assertTrue(allocationFit);
      rootAllocator.verify();

      // Since childAllocator3 now has childAllocator1's buffer, 1, can close
      ArrowBuf1.release();
      childAllocator1.close();
      rootAllocator.verify();

      ArrowBuf2.release();
      childAllocator2.close();
      rootAllocator.verify();

      final BufferAllocator childAllocator4 = rootAllocator.newChildAllocator("transferShared4", 0, MAX_ALLOCATION);
      TransferResult result2 = ArrowBuf3.transferOwnership(childAllocator4);
      allocationFit = result.allocationFit;
      final ArrowBuf ArrowBuf4 = result2.buffer;
      assertTrue(allocationFit);
      rootAllocator.verify();

      ArrowBuf3.release();
      childAllocator3.close();
      rootAllocator.verify();

      ArrowBuf4.release();
      childAllocator4.close();
      rootAllocator.verify();
    }
  }

  @Test
  public void testAllocator_unclaimedReservation() throws Exception {
    try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {
      try (final BufferAllocator childAllocator1 =
          rootAllocator.newChildAllocator("unclaimedReservation", 0, MAX_ALLOCATION)) {
        try(final AllocationReservation reservation = childAllocator1.newReservation()) {
          assertTrue(reservation.add(64));
        }
        rootAllocator.verify();
      }
    }
  }

  @Test
  public void testAllocator_claimedReservation() throws Exception {
    try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {

      try (final BufferAllocator childAllocator1 = rootAllocator.newChildAllocator("claimedReservation", 0,
          MAX_ALLOCATION)) {

        try (final AllocationReservation reservation = childAllocator1.newReservation()) {
          assertTrue(reservation.add(32));
          assertTrue(reservation.add(32));

          final ArrowBuf ArrowBuf = reservation.allocateBuffer();
          assertEquals(64, ArrowBuf.capacity());
          rootAllocator.verify();

          ArrowBuf.release();
          rootAllocator.verify();
        }
        rootAllocator.verify();
      }
    }
  }

  @Test
  public void multiple() throws Exception {
    final String owner = "test";
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {

      final int op = 100000;

      BufferAllocator frag1 = allocator.newChildAllocator(owner, 1500000, Long.MAX_VALUE);
      BufferAllocator frag2 = allocator.newChildAllocator(owner, 500000, Long.MAX_VALUE);

      allocator.verify();

      BufferAllocator allocator11 = frag1.newChildAllocator(owner, op, Long.MAX_VALUE);
      ArrowBuf b11 = allocator11.buffer(1000000);

      allocator.verify();

      BufferAllocator allocator12 = frag1.newChildAllocator(owner, op, Long.MAX_VALUE);
      ArrowBuf b12 = allocator12.buffer(500000);

      allocator.verify();

      BufferAllocator allocator21 = frag1.newChildAllocator(owner, op, Long.MAX_VALUE);

      allocator.verify();

      BufferAllocator allocator22 = frag2.newChildAllocator(owner, op, Long.MAX_VALUE);
      ArrowBuf b22 = allocator22.buffer(2000000);

      allocator.verify();

      BufferAllocator frag3 = allocator.newChildAllocator(owner, 1000000, Long.MAX_VALUE);

      allocator.verify();

      BufferAllocator allocator31 = frag3.newChildAllocator(owner, op, Long.MAX_VALUE);
      ArrowBuf b31a = allocator31.buffer(200000);

      allocator.verify();

      // Previously running operator completes
      b22.release();

      allocator.verify();

      allocator22.close();

      b31a.release();
      allocator31.close();

      b12.release();
      allocator12.close();

      allocator21.close();

      b11.release();
      allocator11.close();

      frag1.close();
      frag2.close();
      frag3.close();

    }
  }
}
