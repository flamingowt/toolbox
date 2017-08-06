package utils.HBaseUtil

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HConstants, HTableDescriptor}

object HBaseUtilNew {
  def createHBaseConfMap:collection.mutable.Map[String,String]={
    val cf=collection.mutable.Map[String,String]()
    cf.put("hbase.master.info.bindAddress","0.0.0.0")
    cf.put("hbase.hstore.flush.retries.number","120")
    cf.put("hbase.client.keyvalue.maxsize","10485760")
    cf.put("base.hstore.compactionThreshold","3")
    cf.put("hbase.master.cleaner.interval","300000")
    cf.put("hbase.zookeeper.property.clientPort","2181")
    cf.put("hbase.regionserver.handler.count","60")
    cf.put("hbase.regionserver.global.memstore.lowerLimit","0.38")
    cf.put("hbase.hregion.memstore.block.multiplier","2")
    cf.put("hbase.hregion.memstore.flush.size","134217728")
    cf.put("hbase.rootdir","hdfs://sotocyon/apps/hbase/data")
    cf.put("hbase.snapshot.enabled","true")
    cf.put("hbase.regionserver.global.memstore.upperLimit","0.4")
    cf.put("zookeeper.session.timeout","120000")
    cf.put("hbase.client.scanner.caching","100")
    cf.put("hbase.tmp.dir","/opt/hadoop/hbase")
    cf.put("hbase.hregion.max.filesize","10737418240")
    cf.put("hfile.block.cache.size","0.40")
    cf.put("hbase.security.authentication","simple")
    cf.put("hbase.defaults.for.version.skip","true")
    cf.put("hbase.master.info.port","60010")
    cf.put(HConstants.ZOOKEEPER_QUORUM,"srm.heracles.sohuno.com,snn1.heracles.sohuno.com,snn2.heracles.sohuno.com")
    cf.put("hbase.regionserver.info.port","60030")
    cf.put("zookeeper.znode.parent","/hbase-unsecure")
    cf.put("hbase.hstore.blockingStoreFiles","10")
    cf.put("hbase.hregion.majorcompaction","86400000")
    cf.put("hbase.security.authorization","false")
    cf.put("hbase.local.dir","/opt/hadoop/hbase/local")
    cf.put("hbase.master.balancer.regionLocationCacheTime","14400")
    cf.put("hbase.cluster.distributed","true")
    cf.put("hbase.hregion.memstore.mslab.enabled","true")
    cf.put("dfs.domain.socket.path","/var/lib/hadoop-hdfs/dn_socket")
    cf.put("hbase.zookeeper.useMulti","true")
    cf
  }

  def createPut(key:String,cf:String,q:String,v:String):Put={
    val put:Put=new Put(Bytes.toBytes(key))
    put.add(Bytes.toBytes(cf),Bytes.toBytes(q),Bytes.toBytes(v))
  }

  @throws[IOException]
  def create(tableName: String, columnFamily: String) {
    val admin: HBaseAdmin = new HBaseAdmin(getConfiguration)
    if (admin.tableExists(tableName)) System.out.println("table exists!")
    else {
      val tableDesc: HTableDescriptor = new HTableDescriptor(tableName)
      tableDesc.addFamily(new HColumnDescriptor(columnFamily))
      admin.createTable(tableDesc)
      System.out.println("create table success!")
    }
  }

  private def getConfiguration: Configuration = {
    val conf: Configuration = HBaseConfiguration.create
    conf.addResource(new Path("/usr/lib/hbase/conf/hbase-site.xml"))
    conf.set("dfs.socket.timeout", "180000")
    conf
  }
  def setHBaseConfig(cf: Configuration): Unit ={
    createHBaseConfMap.map{
      case(k,v)=>{
        cf.set(k,v)
      }
    }
  }
}
