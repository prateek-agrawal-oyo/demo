package com.example.demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController(value = "//")
public class Controller {

  @GetMapping("/ping")
  public String ping() {
    return "pong2";
  }

  line 141
  line 20
  line 15
  line 1
  line 15 1
  line 16
}