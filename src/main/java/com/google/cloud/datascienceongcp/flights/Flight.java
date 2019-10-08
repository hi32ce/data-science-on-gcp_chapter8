package com.google.cloud.datascienceongcp.flights;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class Flight {
  public enum INPUTCORS {
    FL_DATE, UNIQUE_CARRIER, AIRLINE_ID, CARRIER, FL_NUM, ORIGIN_AIRPORT_ID, ORIGIN_AIRPORT_SEQ_ID, ORIGIN_CITY_MARKET_ID, ORIGIN, DEST_AIRPORT_ID, DEST_AIRPORT_SEQ_ID, DEST_CITY_MARKET_ID, DEST, CRS_DEP_TIME, DEP_TIME, DEP_DELAY, TAXI_OUT, WHEELS_OFF, WHEELS_ON, TAXI_IN, CRS_ARR_TIME, ARR_TIME, ARR_DELAY, CANCELLED, CANCELLATION_CODE, DIVERTED, DISTANCE, DEP_AIRPORT_LAT, DEP_AIRPORT_LON, DEP_AIRPORT_TZOFFSET, ARR_AIRPORT_LAT, ARR_AIRPORT_LON, ARR_AIRPORT_TZOFFSET, EVENT, NOTIFY_TIME
  }

  private String[] fields;
  private float avgDepatureDelay, avgArrivalDelay;

  public String getField(INPUTCORS inputcor) {
    return fields[inputcor.ordinal()];
  }

  public boolean isNotDiverted() {
    var col = getField(INPUTCORS.DIVERTED);
    return col.length() == 0 || col.equals("0.00");
  }

  public boolean isNotCancelled() {
    var col = getField(INPUTCORS.CANCELLED);
    return col.length() == 0 || col.equals("0.00");
  }

  public float[] getFloatFeatures() {
    var result = new float[5];
    var col = 0;
    result[col++] = Float.parseFloat(fields[INPUTCORS.DEP_DELAY.ordinal()]);
    result[col++] = Float.parseFloat(fields[INPUTCORS.TAXI_OUT.ordinal()]);
    result[col++] = Float.parseFloat(fields[INPUTCORS.DISTANCE.ordinal()]);
    result[col++] = avgDepatureDelay;
    result[col++] = avgArrivalDelay;
    return result;
  }

  public String toTrainingCsv() {
    var features = this.getFloatFeatures();
    var arrivalDelay = Float.parseFloat(fields[INPUTCORS.ARR_DELAY.ordinal()]);
    var ontime = arrivalDelay < 15;
    var sb = new StringBuilder();
    sb.append(ontime ? 1.0 : 0.0);
    sb.append(",");
    for (var i = 0; i < features.length; ++i) {
      sb.append(features[i]);
      sb.append(",");
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  public static Flight fromCsv(String line) {
    var f = new Flight();
    f.fields = line.split(",");
    f.avgArrivalDelay = f.avgDepatureDelay = Float.NaN;
    if (f.fields.length == INPUTCORS.values().length) {
      return f;
    }
    return null; // malformed
  }
}
