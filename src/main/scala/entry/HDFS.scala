package entry

import java.sql.Timestamp

case class HDFS( userName:String,opTime:String, fsNamesystem:String, cmd:String,
                ip:String, src:String, dst:String, perm:String, proto:String)
