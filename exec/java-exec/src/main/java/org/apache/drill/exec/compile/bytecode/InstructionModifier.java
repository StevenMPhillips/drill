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
package org.apache.drill.exec.compile.bytecode;

import java.util.HashMap;

import org.apache.drill.exec.compile.CompilationConfig;
import org.apache.drill.exec.compile.bytecode.ValueHolderIden.ValueHolderSub;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.FieldInsnNode;

import com.carrotsearch.hppc.IntIntOpenHashMap;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;

public class InstructionModifier extends MethodVisitor {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InstructionModifier.class);

  /* Map from old (reference) local variable index to new local variable information. */
  private final IntObjectOpenHashMap<ValueHolderIden.ValueHolderSub> oldToNew = new IntObjectOpenHashMap<>();

  private final IntIntOpenHashMap oldLocalToFirst = new IntIntOpenHashMap();

  private final DirectSorter adder;
  int lastLineNumber = 0;
  private final TrackingInstructionList list;
  private final String name;
  private final String desc;
  private final String signature;

  private int stackIncrease; // how much larger we have to make the stack

  public InstructionModifier(int access, String name, String desc, String signature, String[] exceptions,
      TrackingInstructionList list, MethodVisitor inner) {
    super(CompilationConfig.ASM_API_VERSION, new DirectSorter(access, desc, inner));
    this.name = name;
    this.desc = desc;
    this.signature = signature;
    this.list = list;
    this.adder = (DirectSorter) mv;

    stackIncrease = 0;
  }

  private ReplacingBasicValue local(int var) {
    Object o = list.currentFrame.getLocal(var);
    if (o instanceof ReplacingBasicValue) {
      return (ReplacingBasicValue) o;
    }
    return null;
  }

  private ReplacingBasicValue popCurrent() {
    return popCurrent(false);
  }

  private ReplacingBasicValue popCurrent(boolean includeReturnVals) {
    // for vararg, we could try to pop an empty stack. TODO: handle this better.
    if (list.currentFrame.getStackSize() == 0) {
      return null;
    }

    Object o = list.currentFrame.pop();
    if (o instanceof ReplacingBasicValue) {
      ReplacingBasicValue v = (ReplacingBasicValue) o;
      if (!v.isFunctionReturn || includeReturnVals) {
        return v;
      }
    }
    return null;
  }

  private ReplacingBasicValue getReturn() {
    Object o = list.nextFrame.getStack(list.nextFrame.getStackSize() - 1);
    if (o instanceof ReplacingBasicValue) {
      return (ReplacingBasicValue) o;
    }
    return null;
  }

  @Override
  public void visitInsn(int opcode) {
    switch (opcode) {
    case Opcodes.DUP:
      /*
       * Pattern:
       *   BigIntHolder out5 = new BigIntHolder();
       *
       * Original bytecode:
       *   NEW org/apache/drill/exec/expr/holders/BigIntHolder
       *   DUP
       *   INVOKESPECIAL org/apache/drill/exec/expr/holders/BigIntHolder.<init> ()V
       *   ASTORE 6 # the index of the out5 local variable (which is a reference)
       *
       * Desired replacement:
       *   ICONST_0
       *   ISTORE 12
       *
       *   In the original, the holder's objectref will be used twice: once for the
       *   constructor call, and then to be stored. Since the holder has been replaced
       *   with one or more locals to hold its members, we don't allocate or store it.
       *   he NEW and the ASTORE are replaced via some stateless changes elsewhere; here
       *   we need to eliminate the DUP.
       *
       * TODO: there may be other reasons for a DUP to appear in the instruction stream,
       * such as reuse of a common subexpression that the compiler optimizer has
       * eliminated. This pattern may also be used for non-holders. We need to be
       * more certain of the source of the DUP, and whether the surrounding context matches
       * the above.
       */
      if (popCurrent() != null) {
        return; // don't emit the DUP
      }

    case Opcodes.DUP_X1:
    case Opcodes.DUP2_X1: {
      /*
       * Pattern:
       *   129:        out.start = out.end = text.end;
       *
       * Original bytecode:
       *   L9
       *    LINENUMBER 129 L9
       *    ALOAD 7
       *    ALOAD 7
       *    ALOAD 8
       *    GETFIELD org/apache/drill/exec/expr/holders/VarCharHolder.end : I
       *    DUP_X1
       *    PUTFIELD org/apache/drill/exec/expr/holders/VarCharHolder.end : I
       *    PUTFIELD org/apache/drill/exec/expr/holders/VarCharHolder.start : I
       *
       * Desired replacement:
       *   L9
       *    LINENUMBER 129 L9
       *    ILOAD 17
       *    DUP
       *    ISTORE 14
       *    ISTORE 13
       *
       *    At this point, the ALOAD/GETFIELD and ALOAD/PUTFIELD combinations have
       *    been replaced by the ILOAD and ISTOREs. However, there is still the DUP_X1
       *    in the instruction stream. In this case, it is duping the fetched holder
       *    member so that it can be stored again. We still need to do that, but because
       *    the ALOADed objectrefs are no longer on the stack, we don't need to duplicate
       *    the value lower down in the stack anymore, but can instead DUP it where it is.
       *    (Similarly, if the fetched field was a long or double, the same applies for
       *    the expected DUP2_X1.)
       *
       * There's also a similar pattern for zeroing values:
       * Pattern:
       *   170:            out.start = out.end = 0;
       *
       * Original bytecode:
       *   L20
       *    LINENUMBER 170 L20
       *    ALOAD 13
       *    ALOAD 13
       *    ICONST_0
       *    DUP_X1
       *    PUTFIELD org/apache/drill/exec/expr/holders/VarCharHolder.end : I
       *    PUTFIELD org/apache/drill/exec/expr/holders/VarCharHolder.start : I
       *
       * Desired replacement:
       *   L20
       *    LINENUMBER 170 L20
       *    ICONST_0
       *    DUP
       *    ISTORE 17
       *    ISTORE 16
       *
       *
       * There's also another pattern that involves DUP_X1
       * Pattern:
       *   1177:                   out.buffer.setByte(out.end++, currentByte);
       *
       * We're primarily interested in the out.end++ -- the post-increment of
       * a holder member.
       *
       * Original bytecode:
       *    L694
       *     LINENUMBER 1177 L694
       *     ALOAD 212
       *     GETFIELD org/apache/drill/exec/expr/holders/VarCharHolder.buffer : Lio/netty/buffer/DrillBuf;
       *     ALOAD 212
       *     DUP
       * >   GETFIELD org/apache/drill/exec/expr/holders/VarCharHolder.end : I
       * >   DUP_X1
       * >   ICONST_1
       * >   IADD
       * >   PUTFIELD org/apache/drill/exec/expr/holders/VarCharHolder.end : I
       *     ILOAD 217
       *     INVOKEVIRTUAL io/netty/buffer/DrillBuf.setByte (II)Lio/netty/buffer/ByteBuf;
       *     POP
       *
       * This fragment includes the entirety of the line 1177 above, but we're only concerned with
       * the lines marked with '>' on the left; the rest were kept for context, because it is the
       * use of the pre-incremented value as a function argument that is generating the DUP_X1 --
       * the DUP_X1 is how the value is preserved before incrementing.
       *
       */
      // were the next two original instructions PUTFIELDs to holders?
      final AbstractInsnNode nextInsn = list.currentInsn.getNext();
      if ((nextInsn.getOpcode() == Opcodes.PUTFIELD) &&
          ScalarReplacementTypes.isHolder(((FieldInsnNode) nextInsn).owner.replace('/','.'))) {
        final AbstractInsnNode nextInsn2 = nextInsn.getNext();
        if ((nextInsn2.getOpcode() == Opcodes.PUTFIELD) &&
            ScalarReplacementTypes.isHolder(((FieldInsnNode) nextInsn2).owner.replace('/','.'))) {
            // we've found the multiple assignment pattern
            super.visitInsn(opcode == Opcodes.DUP_X1 ? Opcodes.DUP : Opcodes.DUP2);
            return;
        }
      }

      // does the next original instruction load a constant?
      if (isXconst(nextInsn.getOpcode())) {
        final AbstractInsnNode nextInsn2 = nextInsn.getNext();
        if (isXadd(nextInsn2.getOpcode())) {
          final AbstractInsnNode nextInsn3 = nextInsn2.getNext();
          if (nextInsn3.getOpcode() == Opcodes.PUTFIELD) {
            final FieldInsnNode putField3 = (FieldInsnNode) nextInsn3;
            if (ScalarReplacementTypes.isHolder(putField3.owner.replace('/', '.'))) {
              // we've found the post-incremented holder member pattern
              super.visitInsn(opcode == Opcodes.DUP_X1 ? Opcodes.DUP : Opcodes.DUP2);
              return;
            }
          }
        }
      }
      }
    }

    // if we get here, emit the original instruction
    super.visitInsn(opcode);
  }

  private static boolean isXconst(final int opcode) {
    switch(opcode) {
    case Opcodes.ICONST_0:
    case Opcodes.ICONST_1:
    case Opcodes.ICONST_2:
    case Opcodes.ICONST_3:
    case Opcodes.ICONST_4:
    case Opcodes.ICONST_5:
    case Opcodes.ICONST_M1:
    case Opcodes.DCONST_0:
    case Opcodes.DCONST_1:
    case Opcodes.FCONST_0:
    case Opcodes.FCONST_1:
    case Opcodes.LCONST_0:
    case Opcodes.LCONST_1:
      return true;
    }

    return false;
  }

  private static boolean isXadd(final int opcode) {
    switch(opcode) {
    case Opcodes.IADD:
    case Opcodes.DADD:
    case Opcodes.FADD:
    case Opcodes.LADD:
      return true;
    }

    return false;
  }

  @Override
  public void visitTypeInsn(int opcode, String type) {
    /*
     * This includes NEW, NEWARRAY, CHECKCAST, or INSTANCEOF.
     *
     * TODO: aren't we just trying to eliminate NEW (and possibly NEWARRAY)?
     * It looks like we'll currently pick those up because we'll only have
     * replaced the values for those, but we might find other reasons to replace
     * things, in which case this will be too broad.
     */
    ReplacingBasicValue r = getReturn();
    if (r != null) {
      ValueHolderSub sub = r.getIden().getHolderSub(adder);
      oldToNew.put(r.getIndex(), sub);
    } else {
      super.visitTypeInsn(opcode, type);
    }
  }

  @Override
  public void visitLineNumber(int line, Label start) {
    lastLineNumber = line;
    super.visitLineNumber(line, start);
  }

  @Override
  public void visitVarInsn(final int opcode, final int var) {
    ReplacingBasicValue v;
    if (opcode == Opcodes.ASTORE && (v = popCurrent(true)) != null) {
      if (!v.isFunctionReturn) {
        final ValueHolderSub from = oldToNew.get(v.getIndex());

        final ReplacingBasicValue current = local(var);
        // if local var is set, then transfer to it to the existing holders in the local position.
        if (current != null) {
          final ValueHolderSub newSub = oldToNew.get(current.getIndex());
          if (newSub.iden() == from.iden()) {
            final int targetFirst = newSub.first();
            from.transfer(this, targetFirst);
            return;
          }
        }

        // if local var is not set, then check map to see if existing holders are mapped to local var.
        if (oldLocalToFirst.containsKey(var)) {
          final ValueHolderSub sub = oldToNew.get(oldLocalToFirst.lget());
          if (sub.iden() == from.iden()) {
            // if they are, then transfer to that.
            from.transfer(this, sub.first());
            return;
          }
        }


        // map from variables to global space for future use.
        oldLocalToFirst.put(var, v.getIndex());

      } else {
        // this is storage of a function return, we need to map the fields to the holder spots.
        int first;
        final ValueHolderIden vIden = v.getIden();
        if (oldLocalToFirst.containsKey(var)) {
          first = oldToNew.get(oldLocalToFirst.lget()).first();
          vIden.transferToLocal(adder, first);
        } else {
          first = vIden.createLocalAndTransfer(adder);
        }

        final ValueHolderSub from = vIden.getHolderSubWithDefinedLocals(first);
        oldToNew.put(v.getIndex(), from);
        v.disableFunctionReturn();
      }

    } else if (opcode == Opcodes.ALOAD && (v = getReturn()) != null) {
      //  Not forwarding this removes a now unnecessary ALOAD for a holder.
    } else {
      super.visitVarInsn(opcode, var);
    }
  }

  void directVarInsn(final int opcode, final int var) {
    adder.directVarInsn(opcode, var);
  }

  @Override
  public void visitMaxs(final int maxStack, final int maxLocals) {
    super.visitMaxs(maxStack + stackIncrease, maxLocals);
  }

  @Override
  public void visitFieldInsn(final int opcode, final String owner, final String name, final String desc) {
    ReplacingBasicValue v;

    switch (opcode) {
    case Opcodes.PUTFIELD:
      // pop twice for put.
      v = popCurrent(true);
      if (v != null) {
        if (v.isFunctionReturn) {
          super.visitFieldInsn(opcode, owner, name, desc);
          return;
        } else {
          // we are trying to store a replaced variable in an external context, we need to generate an instance and
          // transfer it out.
          final ValueHolderSub sub = oldToNew.get(v.getIndex());
          final int additionalStack = sub.transferToExternal(adder, owner, name, desc);
          if (additionalStack > stackIncrease) {
            stackIncrease = additionalStack;
          }
          return;
        }
      }

    case Opcodes.GETFIELD:
      // pop again.
      v = popCurrent();
      if (v != null) {
        // super.visitFieldInsn(opcode, owner, name, desc);
        ValueHolderSub sub = oldToNew.get(v.getIndex());
        sub.addInsn(name, this, opcode);
        return;
      }
    }

    super.visitFieldInsn(opcode, owner, name, desc);
  }

  @Override
  public void visitMethodInsn(int opcode, String owner, String name, String desc) {
    /*
     * This method was deprecated in the switch from api version ASM4 to ASM5.
     * If we ever go back (via CompilationConfig.ASM_API_VERSION), we need to
     * duplicate the work from the other overloaded version of this method.
     */
    assert CompilationConfig.ASM_API_VERSION == Opcodes.ASM4;
    throw new RuntimeException("this method is deprecated");
  }

  @Override
  public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
    assert CompilationConfig.ASM_API_VERSION != Opcodes.ASM4;

    final int len = Type.getArgumentTypes(desc).length;
    final boolean isStatic = opcode == Opcodes.INVOKESTATIC;

    ReplacingBasicValue obj = popCurrent();

    if (obj != null && !isStatic) {
      if ("<init>".equals(name)) {
        // TODO: how do we know this is a constructor for a holder vs something else?
        oldToNew.get(obj.getIndex()).init(adder);
      } else {
        throw new IllegalStateException("you can't call a method on a value holder.");
      }
      return;
    }

    obj = getReturn();

    if (obj != null) {
      /*
       * The return of this method is an actual instance of the object we're escaping.
       * Update so that it gets mapped correctly.
       */
      super.visitMethodInsn(opcode, owner, name, desc, itf);
      obj.markFunctionReturn();
      return;
    }

    int i = isStatic ? 1 : 0;
    for (; i < len; i++) {
      checkArg(name, popCurrent());
    }

    super.visitMethodInsn(opcode, owner, name, desc, itf);
  }

  private void checkArg(String name, ReplacingBasicValue obj) {
    if (obj == null) {
      return;
    }
    throw new IllegalStateException(
        String
            .format(
                "Holder types are not allowed to be passed between methods.  Ran across problem attempting to invoke method '%s' on line number %d",
                name, lastLineNumber));
  }

  @Override
  public void visitEnd() {
    if (logger.isDebugEnabled()) {
      final StringBuilder sb = new StringBuilder();
      sb.append("InstructionModifier ");
      sb.append(name);
      sb.append(' ');
      sb.append(signature);
      sb.append('\n');
      if ((desc != null) && !desc.isEmpty()) {
        sb.append("  desc: ");
        sb.append(desc);
        sb.append('\n');
      }

      int idenId = 0; // used to generate unique ids for the ValueHolderIden's
      int itemCount = 0; // counts up the number of items found
      final HashMap<ValueHolderIden, Integer> seenIdens = new HashMap<>(); // iden -> idenId
      sb.append(" .oldToNew:\n");
      for (IntObjectCursor<ValueHolderIden.ValueHolderSub> ioc : oldToNew) {
        final ValueHolderIden iden = ioc.value.iden();
        if (!seenIdens.containsKey(iden)) {
          seenIdens.put(iden, ++idenId);
          sb.append("ValueHolderIden[" + idenId + "]:\n");
          iden.dump(sb);
        }

        sb.append("  " + ioc.key + " => " + ioc.value + '[' + seenIdens.get(iden) + "]\n");
        ++itemCount;
      }

      sb.append(" .oldLocalToFirst:\n");
      for (IntIntCursor iic : oldLocalToFirst) {
        sb.append("  " + iic.key + " => " + iic.value + '\n');
        ++itemCount;
      }

      if (itemCount > 0) {
        logger.debug(sb.toString());
      }
    }

    super.visitEnd();
  }
}
