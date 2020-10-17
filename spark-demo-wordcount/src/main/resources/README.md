###案例需求：
1. 对文本文件内的每个单词都统计出其出现的次数。
2. 按照每个单词出现次数的数量，降序排序。

###步骤：
1. 创建RDD
2. 将文本进行拆分 （flatMap)
3. 将拆分后的单词进行统计 (mapToPair,reduceByKey)
4. 反转键值对 (mapToPair)
5. 按键升序排序 (sortedByKey)
6. 再次反转键值对 (mapToPair)
7. 打印输出(foreach)

