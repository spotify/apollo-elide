package com.spotify.apollo.elide.testmodel;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class Thing {
  @Id
  public String id;

  public String name;
  public String description = "description";

  public Thing() {
  }

  public Thing(String id, String name) {
    this.id = id;
    this.name = name;
  }
}
