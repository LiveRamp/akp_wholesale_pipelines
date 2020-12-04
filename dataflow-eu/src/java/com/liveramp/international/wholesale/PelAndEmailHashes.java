package com.liveramp.international.wholesale;

import java.io.Serializable;

public class PelAndEmailHashes implements Serializable {

  final String pel;
  final String sha256;
  final String sha1;
  final String md5;

  public PelAndEmailHashes(String pel, String md5email, String sha1Email, String sha256Email) {
    this.pel = pel;
    this.md5 = md5email;
    this.sha1 = sha1Email;
    this.sha256 = sha256Email;
  }

  public String getPel() {
    return pel;
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
