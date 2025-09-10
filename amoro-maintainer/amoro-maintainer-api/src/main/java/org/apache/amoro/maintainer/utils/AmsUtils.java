package org.apache.amoro.maintainer.utils;

import org.apache.amoro.maintainer.api.TableMaintainerConfig;

public class AmsUtils {

  public static String getAmsMaintainerUrl(TableMaintainerConfig config) {
    return config.getAmsUrl() + "/" + config.getAmsMaintainerPort();
  }

  public static String getAmsMetaDataUrl(TableMaintainerConfig config) {
    return config.getAmsUrl() + "/" + config.getAmsOptimizerPort();
  }
}
