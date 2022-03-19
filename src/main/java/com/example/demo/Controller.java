package com.example.demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController(value = "/")
public class Controller {

  @GetMapping("/ping")
  public String ping() {
    return "pong";
  }
}