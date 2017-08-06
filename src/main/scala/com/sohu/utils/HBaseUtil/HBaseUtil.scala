package utils.HBaseUtil

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

object HBaseUtil {

  def createHBaseConfMap:collection.mutable.Map[String,String]={
    val cf=collection.mutable.Map[String,String]()
    cf.put(HConstants.HBASE_CLIENT_PAUSE, "3000")
    cf.put(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "5")
    cf.put(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "60000")

    cf.put(HConstants.ZOOKEEPER_QUORUM,"dn074021.heracles.sohuno.com,kb.heracles.sohuno.com,monitor.heracles.sohuno.com")

    cf.put("hbase.master.keytab.file","/etc/security/keytabs/hbase.service.keytab")
    cf.put("hbase.master.kerberos.principal","hbase/_HOST@HERACLES.SOHUNO.COM")
    cf.put("hbase.master.info.bindAddress","0.0.0.0")
    cf.put("hbase.master.info.port","60010")

    cf.put("hbase.regionserver.keytab.file","/etc/security/keytabs/hbase.service.keytab")
    cf.put("hbase.regionserver.kerberos.principal","hbase/_HOST@HERACLES.SOHUNO.COM")
    cf.put("hbase.regionserver.info.port","60030")

    cf.put("zookeeper.znode.parent","/hbase-secure")
    cf.put("hbase.security.authentication","kerberos")
    cf.put("hbase.security.authorization","true")
    cf.put("hbase.coprocessor.region.classes","org.apache.hadoop.hbase.security.token.TokenProvider,org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint,org.apache.hadoop.hbase.security.access.AccessController")

    cf.put("hbase.tmp.dir","/opt/hadoop/hbase")
    cf.put("hbase.rootdir","hdfs://heracles/apps/hbase/data")
    cf.put("hbase.superuser","hbase")
    cf.put("hbase.zookeeper.property.clientPort","2181")
    cf.put("hbase.cluster.distributed","true")
    cf
  }

  def createPut(key:String,cf:String,q:String,v:String):Put={
    val put:Put=new Put(Bytes.toBytes(key))
    put.add(Bytes.toBytes(cf),Bytes.toBytes(q),Bytes.toBytes(v))
  }

  def setHBaseConfig(cf: Configuration): Unit ={
    createHBaseConfMap.map{
      case(k,v)=>{
        cf.set(k,v)
      }
    }
  }
}
