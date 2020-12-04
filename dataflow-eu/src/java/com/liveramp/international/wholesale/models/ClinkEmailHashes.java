package com.liveramp.international.wholesale.models;

import java.io.Serializable;
import java.util.Objects;

public class ClinkEmailHashes implements Serializable {

  private final String key;
  private final String sha256;
  private final String sha1;
  private final String md5;

  public ClinkEmailHashes(String key, String md5email, String sha1Email, String sha256Email) {
    this.key = key;
    this.md5 = md5email;
    this.sha1 = sha1Email;
    this.sha256 = sha256Email;
  }

  @Override
  public String toString() {
    return "EmailHashes{" +
        "key='" + key + '\'' +
        ", sha256='" + sha256 + '\'' +
        ", sha1='" + sha1 + '\'' +
        ", md5='" + md5 + '\'' +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClinkEmailHashes that = (ClinkEmailHashes) o;
    return Objects.equals(key, that.key) &&
        Objects.equals(sha256, that.sha256) &&
        Objects.equals(sha1, that.sha1) &&
        Objects.equals(md5, that.md5);
  }

  public String getKey() {
    return key;
  }

  public String getSha256() {
    return sha256;
  }

  public String getSha1() {
    return sha1;
  }

  public String getMd5() {
    return md5;
  }
}