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
package org.apache.drill.exec;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.builder.EqualsBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class GenerateJson {

  public static void main(String[] args) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    int iterations = 100000;
    String fileName = "/tmp/lists2.json";
    final FileOutputStream output = new FileOutputStream(fileName);
    Map<String,Object> data = new HashMap<>();
    Object obj1 = null;
    for (int i = 0; i < iterations; i++) {
      OutputStream os = new OutputStream() {

        @Override
        public void write(int b) throws IOException {
          output.write(b);
          System.out.write(b);
        }
      };
      obj1 = mapValue(3);
//      data.put("data", obj1);
      mapper.writer().writeValue(os, obj1);
      output.write("\n".getBytes());
      System.out.println();
    }
    output.close();
  }

  private static Object readObjects(String file, ObjectMapper mapper) throws IOException {
    final FileInputStream fileInputStream = new FileInputStream(file);
    InputStream is = new InputStream() {
      @Override
      public int read() throws IOException {
        return fileInputStream.read();
      }
    };
    JsonFactory factory = new JsonFactory();
    JsonParser parser = factory.createParser(is);
    Map<String,Object> object = mapper.reader().readValue(parser, new TypeReference<Map<String, Object>>() {
    });
    return object;
  }

  private enum Type {
    INT, FLOAT, STRING, MAP, LIST
  }

  private static Type[] types = Type.values();

  private static Type randomType() {
    int r = ThreadLocalRandom.current().nextInt(types.length);
    return types[r];
  }

  private static Object getNewObject(int maxDepth) {
    Type type = randomType();
    switch (type) {
    case INT:
      return intValue();
    case FLOAT:
      return floatValue();
    case STRING:
      return stringValue();
    case MAP:
      return mapValue(maxDepth);
    case LIST:
      return listValue(maxDepth);
    default:
      return null;
    }
  }

  private static Map<String, Object> mapValue(int depth) {
    if (depth < 0) {
      return null;
    }
    Map<String,Object> map = new LinkedHashMap<>();
    Collections.shuffle(keys);
    int length = ThreadLocalRandom.current().nextInt(3);
    List<String> keyList = keys.subList(0, length);

    for (int i = 0; i < length; i++) {
      map.put(keyList.get(i), getNewObject(depth - 1));
    }
    return map;
  }

  private static List<Object> listValue(int depth) {
    if (depth < 0) {
      return null;
    }

    int length = ThreadLocalRandom.current().nextInt(3);

    List<Object> list = Lists.newArrayList();

    for (int i = 0; i < length; i++) {
      Object obj = getNewObject(depth - 1);
      if (obj != null) {
        list.add(obj);
      }
    }
    return list;
  }

  private static Float floatValue() {
    return ThreadLocalRandom.current().nextFloat();
  }

  private static Integer intValue() {
    return ThreadLocalRandom.current().nextInt();
  }

  private static String stringValue() {
    int length = ThreadLocalRandom.current().nextInt(0, 3);
    List<String> w = Lists.newArrayList();
    for (int i = 0; i < length; i++) {
      String nextWord = words.get(ThreadLocalRandom.current().nextInt(words.size()));
      w.add(nextWord);
    }

    return Joiner.on(" ").join(w);
  }

  private static List<String> words = Lists.newArrayList();
  private static List<String> keys = Lists.newArrayList();

  static {
    String fileName = "/usr/share/dict/words";
    BufferedReader wordReader = null;
    try {
      wordReader = new BufferedReader(new FileReader(fileName));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }

    String line;
    try {
      while ((line = wordReader.readLine()) != null) {
        words.add(line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    Collections.shuffle(words);

    keys.addAll(words.subList(0, 3));
  }
}
