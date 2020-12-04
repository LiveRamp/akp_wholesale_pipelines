package com.liveramp.dataflow;

import org.junit.experimental.categories.Categories;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.liveramp.dataflow.arlpel.ConvertRawToArlPelTest;
import com.liveramp.dataflow.arlpel.LoadArlPelBigtableTest;
import com.liveramp.international.bigtable.ClinkLoaderTest;
import com.liveramp.testing.categories.DatabaseRequiredTest;

@RunWith(Categories.class)
@Categories.ExcludeCategory(DatabaseRequiredTest.class)
@Suite.SuiteClasses({ConvertRawToArlPelTest.class, LoadArlPelBigtableTest.class, ClinkLoaderTest.class})
public class DataflowEuTestSuite {

}
