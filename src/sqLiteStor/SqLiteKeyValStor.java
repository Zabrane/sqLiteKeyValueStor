package sqLiteStor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

public class SqLiteKeyValStor<U extends Serializable>{
  private String group;
  private SqLiteKeyValStorBacking backing;
  private Hashtable<String, U> cache;
  private BloomFilter<String> bloom;
  
  /**
   * Creates a new instance of the key value store. Creates or uses an existing disk backing file
   * and loads all current keys in the group from disk.
   * 
   * DO NOT instantiate more than one for the same group and DB file. It will not cause any direct failure or error
   * but because of in memory caching neither will see the other's changes. This will lead to subtle bugs in your code.
   * 
   * You have been warned.
   * 
   * The backing DB file is asynchronous so that puts and updates can return faster, but on program exit you MUST call 
   * {@link sync} to ensure all changes are committed to the database
   * 
   * All functions are thread safe.
   * 
   * @param group
   * @param dbFile
   */
  public SqLiteKeyValStor(String group, String dbFile){
    this.backing = SqLiteKeyValStorBacking.getInstance(dbFile);
    this.group = group;
    this.cache = new Hashtable<String, U>();
    this.bloom = new BloomFilter<String>(256, 10000);
    //To speed up misses we warm the cache
    //This way we can quickly return nulls based on the bloom filter
    this.backing.getAllInGroup(this.group, this.cache);
    new UpdateBloom<String, U>(this.bloom, this.cache).start();
  }
  
  public U get(String key){
    synchronized(this.cache){
      if(!this.bloom.contains(key)){
        return null;
      }
      U obj = this.cache.get(key);
      return obj;
    }
  }
  
  /**
   * 
   * Returns an array of Map Entries. Note that if they are manipulted you will have to sync them externally.
   * 
   * @return array of Map.Entry's 
   */
  @SuppressWarnings("unchecked")
  public Map.Entry<String,U>[] getAllEntryArray(){
    synchronized(this.cache){
      Set<Map.Entry<String,U>> ents = this.cache.entrySet();
      ArrayList<Map.Entry<String,U>> ret = new ArrayList<Map.Entry<String,U>>();
      for(Map.Entry<String,U> ent : ents){
        ret.add(ent);
      }
      return (Map.Entry<String, U>[])ret.toArray();
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
      this.bloom.add(key);
      this.cache.put(key, obj);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream objOut;
      try{
        objOut = new ObjectOutputStream(baos);
        objOut.writeObject(obj);
        objOut.close();
        this.backing.queue.add(new KeyValAction(key, this.group, baos, KVActions.PUT));
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
      U fromCache = cache.get(key);
      if(!(fromCache == null)){
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

/**
 * 
 * @author \\
 *
 * Every 60 minutes this rebuilds the given bloom filter based on the map given to keep false positives low
 *
 * @param <K>
 * @param <U>
 */

class UpdateBloom<K, U> extends Thread{
  BloomFilter<K> bloom;
  Map<K,U> cache;
  UpdateBloom(BloomFilter<K> bloom, Map<K,U> cache){
    this.bloom = bloom;
    this.cache = cache;
  }
  
  public void run(){
    for(;;){
      try{
        synchronized(this.cache){
          this.bloom.clear();
          for(K key : this.cache.keySet()){
            this.bloom.add(key);
          }
        }
        Thread.sleep(1000*60*60);
      }catch(Exception e){
        e.printStackTrace();
      }
      
    }
  }
}
