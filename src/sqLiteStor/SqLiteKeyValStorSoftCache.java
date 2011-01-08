package sqLiteStor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.lang.ref.SoftReference;

import com.facebook.infrastructure.utils.CountingBloomFilter;


public class SqLiteKeyValStorSoftCache<U extends Serializable>{
  private String group;
  private SqLiteKeyValStorBacking backing;
  private Hashtable<String, SoftReference<U>> cache;
  private CountingBloomFilter bloom;
  
  /**
   * Creates a new instance of the key value store. Creates or uses an existing disk backing file
   * and loads all current keys in the group from disk.
   * 
   * DO NOT instantiate more than one for the same group and DB file. It will not cause any direct failure or error
   * but because of in memory caching neither will see the other's changes. This will lead to subtle bugs in your code.
   * 
   * You have been warned.
   * 
   * The backing DB file is asynchronous so that puts and updates can return faster. The back-end flushes on exit,
   * but be sure that at that point in time nothing is trying to add to the queue. It might not make it.
   * 
   * All functions are thread safe.
   * 
   * @param group
   * @param dbFile
   */
  public SqLiteKeyValStorSoftCache(String group, String dbFile){
    this.backing = SqLiteKeyValStorBacking.getInstance(dbFile);
    this.group = group;
    this.cache = new Hashtable<String, SoftReference<U>>();
    this.bloom = new CountingBloomFilter(1024, 36);
    for(String key : this.backing.getAllKeysInGroup(this.group)){
      this.bloom.add(key);
    }
  }
  
  @SuppressWarnings("unchecked")
  public U get(String key){
    synchronized(this.cache){
      U obj;
      SoftReference<U> ref;
      if((ref = this.cache.get(key)) != null && (obj = ref.get()) != null)){
        return obj;
      }
      if(!this.bloom.isPresent(key)){
        return null;
      }
      obj = (U)this.backing.get(key, this.group);
      if(obj != null){
        this.cache.put(key, new SoftReference<U>(obj));
      }else{
        this.cache.remove(key);
        this.bloom.delete(key);
      }
      return obj;
    }
  }
  
  /**
   * 
   * Returns an array of Map Entries. Note that if they are manipulated you will have to sync them externally.
   * 
   * @return array of Map.Entry's 
   */
  public Map<String,U> getAllEntryArray(){
    synchronized(this.cache){
      Hashtable<String, U> ret = new Hashtable<String, U>();
      this.backing.getAllInGroup(this.group, ret);
      SoftReference<U> obj;
      for(Map.Entry<String,U> ent : ret.entrySet()){
        if((obj = this.cache.get(ent.getKey())) != null){
          ent.setValue(obj.get());
        }else{
          this.cache.put(ent.getKey(), new SoftReference<U>(ent.getValue())); //needed to maintain continuity of soft references. If not for this then a get could return a DIFFERENT instance of what should be the same object!
          this.bloom.add(ent.getKey());
        }
      }
      return ret;
    }
  }
  
  /**
   * Adds an entry to the table and puts a copy on disk. If you have modified an existing entry use {@link update} instead
   * 
   * @param key
   * @param obj
   */
  
  public void put(String key, U obj){
    synchronized(this.cache){
      if(!bloom.isPresent(key)){
        this.bloom.add(key);
      }
      this.cache.put(key, new SoftReference<U>(obj));
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream objOut;
      try{
        objOut = new ObjectOutputStream(baos);
        objOut.writeObject(obj);
        objOut.close();
        this.backing.queue.add(new KeyValAction(key, this.group, baos, KVActions.PUT, obj));
      }catch(IOException e){
        e.printStackTrace();
      }
    }
  }
  
  /**
   * Removes the entry from the cache and disk 
   * @param key
   */
  
  public void remove(String key){
    synchronized(this.cache){
      this.cache.remove(key);
      if(bloom.isPresent(key)){
        this.bloom.delete(key);
      }
      this.backing.queue.add(new KeyValAction(key, this.group, null, KVActions.REMOVE));
    }
  }
  
  /**
   * Updates the entry on-disk. Only requires a key because it assumes you have modified the reference.
   * <p>
   * If you created a whole new reference use {@link put} instead.
   * @see put 
   * @param key
   */
  
  public void update(String key){
    synchronized(this.cache){
      U fromCache = cache.get(key).get();
      if(!(fromCache == null)){ //It must have been removed explicitly. So long as a hard reference exists we won't find a null here for a valid key
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream objOut;
        try{
          objOut = new ObjectOutputStream(baos);
          objOut.writeObject(fromCache);
          objOut.close();
          this.backing.queue.add(new KeyValAction(key, this.group, baos, KVActions.PUT));
        }catch(IOException e){
          e.printStackTrace();
        }
      }
    }
  }
  
  /**
   * Blocks while until the backer's queue is empty.
   * <br>
   * Note that since the backer can be shared there still may be a considerable amount of activity in the queue
   * <p>
   * 
   * Here is how it works: The queue is locked until it is empty and commits to the DB. Then it is unlocked and notifies are sent.
   * <p>
   * At this point the flush returns and any data that was in the queue until the notify was sent is on disk. 
   * <p>
   * Because all methods that can modify the queue are synchronized this means that anything that was added to the queue before the flush is on disk.
   * 
   */
  
  public void flush(){
    synchronized(this.cache){
      this.backing.flush();
    }
  }
  
}

