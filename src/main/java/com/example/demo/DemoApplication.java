package com.example.demo;

import java.io.FileReader;
import java.io.IOException;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.opencsv.CSVReader;

@SpringBootApplication
public class DemoApplication {

  public static void main(String[] args) throws IOException {
    int count = 0;
    try (CSVReader reader = new CSVReader(new FileReader("/Users/prateek.agarwal/Downloads/p.csv"))) {
      while (reader.readNext() != null)
        count++;
      System.out.println(count);
    }
  }

}
