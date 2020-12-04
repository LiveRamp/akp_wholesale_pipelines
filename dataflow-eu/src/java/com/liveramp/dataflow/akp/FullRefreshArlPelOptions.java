package com.liveramp.dataflow.akp;

import java.io.Serializable;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;


public class FullRefreshArlPelOptions implements Serializable {

  private static final long serialVersionUID = -7164048954957372442L;

  private String ana;
  private String inputFile;


  public static long getSerialVersionUID() {
    return serialVersionUID;
  }

  public String getAna() {
    return ana;
  }

  public void setAna(String ana) {
    this.ana = ana;
  }

  public String getInputFile() {
    return inputFile;
  }

  public void setInputFile(String inputFile) {
    this.inputFile = inputFile;
  }

  private static class PropertyExtractorFuntion<T> extends SimpleFunction<FullRefreshArlPelOptions, T> implements
      Serializable {

    private static final long serialVersionUID = -2568928069006704235L;
    private final SerializableFunction<FullRefreshArlPelOptions, T> propertyExtractor;

    public PropertyExtractorFuntion(SerializableFunction<FullRefreshArlPelOptions, T> propertyExtractor) {
      this.propertyExtractor = propertyExtractor;
    }

    @Override
    public T apply(FullRefreshArlPelOptions input) {
      return propertyExtractor.apply(input);
    }

  }
}
