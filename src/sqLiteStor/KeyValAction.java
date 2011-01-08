package sqLiteStor;

import java.io.ByteArrayOutputStream;

enum KVActions{PUT, REMOVE};

public class KeyValAction{
  public KVActions action; 
  public String key;
  public String group;
  public ByteArrayOutputStream value;
  @SuppressWarnings("unused")
  private Object hardRef; //Used for the soft cache to keep the object in memory until it is on the disk
  
  KeyValAction(String key, String group, ByteArrayOutputStream val, KVActions action){
    this.key = key;
    this.group = group;
    this.value = val;
    this.action = action;
  }
  
  KeyValAction(String key, String group, ByteArrayOutputStream val, KVActions action, Object hardRef){
    this.key = key;
    this.group = group;
    this.value = val;
    this.action = action;
    this.hardRef = hardRef;
  } 
}