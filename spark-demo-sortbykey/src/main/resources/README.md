###需求：
1. 按照文件中的第一列排序。
2. 如果第一列相同，则按照第二列排序。

###实现步骤：
1. 实现自定义的key，要实现Ordered接口和Serializable接口，在key中实现自己对多个列的排序算法
2. 将包含文本的RDD，映射成key为自定义key，value为文本的JavaPairRDD（map）
3. 使用sortByKey算子按照自定义的key进行排序（sortByKey）
4. 再次映射，剔除自定义的key，只保留文本行（map）
5. 打印输出（foreach）