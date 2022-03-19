package com.example.demo.simple_unique_insert;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.google.common.collect.Lists;
import com.sun.tools.javac.util.Pair;

@RestController
@RequestMapping("/simple_unique_insert_test")
public class SimpleUniqueInsertController {

  @Value("${spring.datasource.url}")
  private String url;
  @Value("${spring.datasource.username}")
  private String user;
  @Value("${spring.datasource.password}")
  private String password;
  @Autowired
  private SimpleUniqueInsertTestRepo testRepo;

  Connection getC() throws SQLException {
    return DriverManager.getConnection(url, user, password);
  }

  public List<Pair<String, String>> init(int limit) throws SQLException {
    Set<Pair<String, String>> rows = new HashSet<>();

    String SALT_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";
    final int countOfInsert = 100000;
    while (rows.size() < countOfInsert + limit) {
      StringBuilder first = new StringBuilder();
      StringBuilder second = new StringBuilder();
      Random rnd = new Random();

      for (int j = 0; j < 100; j++) {
        first.append(SALT_CHARS.charAt((int) (rnd.nextFloat() * SALT_CHARS.length())));
        second.append(SALT_CHARS.charAt((int) (rnd.nextFloat() * SALT_CHARS.length())));
      }
      rows.add(new Pair<>(first.toString(), second.toString()));
    }

    try (Statement stmt = getC().createStatement()) {
      String sql = "DROP TABLE if exists simple_unique_insert_test";
      stmt.execute(sql);
    }

    try (Statement stmt = getC().createStatement()) {
      String sql = "CREATE TABLE simple_unique_insert_test(\n"
          + "id serial primary key not null, \n"
          + "first varchar(127),\n"
          + "last varchar(127),\n"
          + "UNIQUE (first, last)\n"
          + ");";
      stmt.execute(sql);
    }

    List<Pair<String, String>> totalRows = new ArrayList<>(rows);
    List<Pair<String, String>> list = totalRows.subList(0, limit);
    List<Pair<String, String>> toInsert = totalRows.subList(limit, limit + countOfInsert);

    System.out.printf(
        "totalRows: %s, toInsert: %s, list: %s%n", rows.size(), toInsert.size(), list.size());

    for (List<Pair<String, String>> toInsertBatch : Lists.partition(toInsert, 50000)) {
      try (Statement stmt = getC().createStatement()) {
        stmt.execute("insert into simple_unique_insert_test(first, last) values " +
            toInsertBatch.stream().map(pair -> String.format("('%s', '%s')", pair.fst, pair.snd))
                .collect(Collectors.joining(", "))
        );
      }
    }
    return list;
  }

  @GetMapping(value = "/1")
  public long useQuery(@RequestParam int limit) throws SQLException {
    final List<Pair<String, String>> list = init(limit);
    long startTime = System.currentTimeMillis();
    for (Pair<String, String> pair : list) {
      try (Statement stmt = getC().createStatement()) {
        String sql = String.format(
            "insert into simple_unique_insert_test(first, last) values('%s', '%s')",
            pair.fst, pair.snd);
        stmt.execute(sql);
      }
    }
    return System.currentTimeMillis() - startTime;
  }

  @GetMapping(value = "/2")
  public long useQueryOnSingleConn(@RequestParam int limit) throws SQLException {
    final List<Pair<String, String>> list = init(limit);
    long startTime = System.currentTimeMillis();
    try (Statement stmt = getC().createStatement()) {
      for (Pair<String, String> pair : list) {
        String sql = String.format(
            "insert into simple_unique_insert_test(first, last) values('%s', '%s')",
            pair.fst, pair.snd);
        stmt.execute(sql);
      }
    }
    return System.currentTimeMillis() - startTime;
  }

  @GetMapping(value = "/3")
  public long useQueryJPA(@RequestParam int limit) throws SQLException {
    final List<Pair<String, String>> list = init(limit);
    long startTime = System.currentTimeMillis();

    final List<SimpleUniqueInsertTest> collect = list.stream().map(pair -> new SimpleUniqueInsertTest(pair.fst, pair.snd))
        .collect(Collectors.toList());
    testRepo.saveAll(collect);

    return System.currentTimeMillis() - startTime;
  }

  @GetMapping(value = "/4")
  public long useP(@RequestParam int limit) throws SQLException {
    final List<Pair<String, String>> list = init(limit);
    try (Statement stmt = getC().createStatement()) {
      stmt.execute(
          "CREATE OR REPLACE PROCEDURE public.bulk_simple_unique_insert_test(IN bulk_data xml)\n"
              + " LANGUAGE plpgsql\n"
              + "AS $procedure$\n"
              + "begin\n"
              + "\n"
              + "\t\n"
              + "\tINSERT INTO simple_unique_insert_test (first, last)\n"
              + "\tSELECT unnest(xpath('//f/text()', myTempTable.myXmlColumn)), \n"
              + "\t\t\tunnest(xpath('//l/text()', myTempTable.myXmlColumn))\n"
              + "\tFROM unnest(xpath('//r', bulk_data)) AS myTempTable(myXmlColumn);\n"
              + "\n"
              + "commit;\n"
              + "end;$procedure$\n");
    }
    long startTime = System.currentTimeMillis();
    try (Statement stmt = getC().createStatement()) {
      StringBuilder xml = new StringBuilder();
      for (Pair<String, String> pair : list) {
        xml.append("<r>").append("<f>").append(pair.fst).append("</f>").append("<l>")
            .append(pair.snd).append("</l>").append("</r>");
      }
      String sql = String.format(
          "CALL bulk_simple_unique_insert_test(bulk_data:='<rows>%s</rows>')", xml);
      stmt.execute(sql);
    }
    return System.currentTimeMillis() - startTime;
  }
}