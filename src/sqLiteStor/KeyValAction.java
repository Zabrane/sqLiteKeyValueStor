package sqLiteStor;

import java.io.ByteArrayOutputStream;

enum KVActions{PUT, REMOVE};

public class KeyValAction{
  public KVActions action; 
  public String key;
  public String group;
  public ByteArrayOutputStream value;
  
  KeyValAction(String key, String group, ByteArrayOutputStream val, KVActions action){
    this.key = key;
    this.group = group;
    this.value = val;
    this.action = action;
  } 
}