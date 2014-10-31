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
package org.apache.drill.exec.compile;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.util.TraceClassVisitor;
import org.slf4j.Logger;

/**
 * Utilities commonly used with ASM.
 *
 * <p>There are several class verification utilities which use
 * CheckClassAdapter (DrillCheckClassAdapter) to ensure classes are well-formed;
 * these are packaged as boolean functions so that they can be used in assertions.
 */
public class AsmUtil {
  // This class only contains static utilities.
  private AsmUtil() {
  }

  /**
   * Check to see if a class is well-formed.
   *
   * @param classNode the class to check
   * @param logTag a tag to print to the log if a problem is found
   * @param logger the logger to write to if a problem is found
   * @return true if the class is ok, false otherwise
   */
  public static boolean isClassOk(final ClassNode classNode, final String logTag, final Logger logger) {
    final StringWriter sw = new StringWriter();
    final ClassWriter verifyWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
    classNode.accept(verifyWriter);
    final ClassReader ver = new ClassReader(verifyWriter.toByteArray());
    try {
      DrillCheckClassAdapter.verify(ver, false, new PrintWriter(sw));
    } catch(Exception e) {
      logger.info("Caught exception verifying class:");
      logClass(logTag, classNode, logger);
      throw e;
    }
    final String output = sw.toString();
    if (!output.isEmpty()) {
      logger.info("Invalid class:\n" +  output);
      return false;
    }

    return true;
  }

  /**
   * Check to see if a class is well-formed.
   *
   * @param classBytes the bytecode of the class to check
   * @param logTag a tag to print to the log if a problem is found
   * @param logger the logger to write to if a problem is found
   * @return true if the class is ok, false otherwise
   */
  public static boolean isClassBytesOk(final byte[] classBytes, final String logTag, final Logger logger) {
    final ClassNode classNode = classFromBytes(classBytes);
    return isClassOk(classNode, logTag, logger);
  }

  /**
   * Create a ClassNode from bytecode.
   *
   * @param classBytes the bytecode
   * @return the ClassNode
   */
  public static ClassNode classFromBytes(final byte[] classBytes) {
    final ClassNode classNode = new ClassNode(CompilationConfig.ASM_API_VERSION);
    final ClassReader classReader = new ClassReader(classBytes);
    classReader.accept(classNode, 0);
    return classNode;
  }

  /**
   * Write a class to the log.
   *
   * <p>Writes at level DEBUG.
   *
   * @param logTag a tag to print to the log
   * @param classNode the class
   * @param logger the logger to write to
   */
  public static void logClass(final String logTag, final ClassNode classNode, final Logger logger) {
    logger.debug(logTag);
    final StringWriter stringWriter = new StringWriter();
    final PrintWriter printWriter = new PrintWriter(stringWriter);
    final TraceClassVisitor traceClassVisitor = new TraceClassVisitor(printWriter);
    classNode.accept(traceClassVisitor);
    logger.debug(stringWriter.toString());
  }

  /**
   * Write a class to the log.
   *
   * <p>Writes at level DEBUG.
   *
   * @param logTag a tag to print to the log
   * @param classBytes the class' bytecode
   * @param logger the logger to write to
   */
  public static void logClassFromBytes(final String logTag, final byte[] classBytes, final Logger logger) {
    final ClassNode classNode = classFromBytes(classBytes);
    logClass(logTag, classNode, logger);
  }
}
