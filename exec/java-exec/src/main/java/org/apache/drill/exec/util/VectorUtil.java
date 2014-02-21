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
package org.apache.drill.exec.util;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.RpcException;

import com.beust.jcommander.internal.Lists;

public class VectorUtil {

  public static void showVectorAccessibleContent(VectorAccessible va) {

    int rows = va.getRecordCount();
    List<String> columns = Lists.newArrayList();
    for (VectorWrapper vw : va) {
      columns.add(vw.getValueVector().getField().getName());
    }

    int width = columns.size();
    for (int row = 0; row < rows; row++) {
      if (row%50 == 0) {
        System.out.println(StringUtils.repeat("-", width*17 + 1));
        for (String column : columns) {
          System.out.printf("| %-15s", column.length() <= 15 ? column : column.substring(0, 14));
        }
        System.out.printf("|\n");
        System.out.println(StringUtils.repeat("-", width*17 + 1));
      }
      for (VectorWrapper vw : va) {
        Object o = vw.getValueVector().getAccessor().getObject(row);
        if (o == null) {
          //null value
          System.out.printf("| %-15s", "");
        }
        else if (o instanceof byte[]) {
//          String value = new String((byte[]) o);
          String value = toStringBinary((byte[]) o, 0, ((byte[])o).length);
          System.out.printf("| %-15s",value.length() <= 15 ? value : value.substring(0, 14));
        } else {
          String value = o.toString();
          System.out.printf("| %-15s",value.length() <= 15 ? value : value.substring(0,14));
        }
      }
      System.out.printf("|\n");
    }

    if (rows > 0 )
      System.out.println(StringUtils.repeat("-", width*17 + 1));
  }

  public static String toStringBinary(final byte [] b, int off, int len) {
      StringBuilder result = new StringBuilder();
      try {
          String first = new String(b, off, len, "ISO-8859-1");
          for (int i = 0; i < first.length() ; ++i ) {
           int ch = first.charAt(i) & 0xFF;
           if ( (ch >= '0' && ch <= '9')
               || (ch >= 'A' && ch <= 'Z')
               || (ch >= 'a' && ch <= 'z')
               || " `~!@#$%^&*()-_=+[]{}\\|;:'\",.<>/?".indexOf(ch) >= 0 ) {
               result.append(first.charAt(i));
             } else {
               result.append(String.format("\\x%02X", ch));
             }
         }
        } catch (UnsupportedEncodingException e) {
        }
      return result.toString();
    }


}
