import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.util.Bytes

df
  .repartition(300) // à ajuster selon régions HBase
  .foreachPartition { partition =>

    val conf = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(conf)

    val params = new BufferedMutatorParams(
      TableName.valueOf("ma_table")
    ).writeBufferSize(10 * 1024 * 1024) // 10MB

    val mutator = connection.getBufferedMutator(params)

    partition.foreach { row =>
      val put = new Put(Bytes.toBytes(row.getString(0)))
      put.addColumn(
        Bytes.toBytes("cf"),
        Bytes.toBytes("q"),
        Bytes.toBytes(row.getString(1))
      )
      mutator.mutate(put)
    }

    mutator.flush()
    mutator.close()
    connection.close()
  }
