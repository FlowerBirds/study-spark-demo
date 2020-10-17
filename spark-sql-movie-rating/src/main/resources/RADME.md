###影评案例

1. 求被评分次数最多的 10 部电影，并给出评分次数（电影名，评分次数）
2. 分别求男性，女性当中评分最高的 10 部电影（性别，电影名，影评分）（评论次数必须
达到 50 次）
3. 分别求男性，女性看过最多的 10 部电影（性别，电影名） 
4. 年龄段在”18-24”的男人，最喜欢看 10 部电影
5. 求 movieid = 2116 这部电影各年龄段（因为年龄就只有 7 个，就按这个 7 个分就好了）的平均影评（年龄段，影评分）
6. 求最喜欢看电影（影评次数最多）的那位女性评最高分的 10 部电影（评论次数必须达到50 次，如果最评分相同，请取评论次数多的，否则取评分高的）的平均影评分（观影者，电影名，影评分）
7. 求好片（评分>=4.0）最多的那个年份中的最好看的 10 部电影（评论次数达到 50） 
8. 求 1997 年上映的电影中，评分最高的 10 部 Comedy 类电影（评论次数达到 50） 
9. 该影评库中各种类型电影中评价最高的 5 部电影（类型，电影名，平均影评分）
10. 各年评分最高的电影类型（年份，类型，影评分）


> 需要使用到的数据描述如下(older datasets)
1. users.dat 数据格式为： `2::M::56::16::70072`
对应字段为：UserID BigInt, Gender String, Age Int, Occupation String, Zipcode String
对应字段中文解释：用户 id，性别，年龄，职业，邮政编码
2. movies.dat 数据格式为： `2::Jumanji (1995)::Adventure|Children's|Fantasy`
对应字段为：MovieID BigInt, Title String, Genres String
对应字段中文解释：电影 ID，电影名字，电影类型
3. ratings.dat 数据格式为： `1::1193::5::978300760`
对应字段为：UserID BigInt, MovieID BigInt, Rating Double, Timestamped String
对应字段中文解释：用户 ID，电影 ID，评分，评分时间戳

####整体的设计SQL语句的思路
#####查询思路
1. select ** from xx1 join xx2 join xx3 on where xx group by xx having xx order by xx
2. 可用的一些拓展语句
3. topN:row_number() over() 嵌套子查询 explode 字符串拆分


####数据集
下载链接:  
官网地址: https://grouplens.org/datasets/movielens/  
ml-latest-small(1MB): http://files.grouplens.org/datasets/movielens/ml-latest-small.zip 
ml-latest(234.2MB): http://files.grouplens.org/datasets/movielens/ml-latest.zip
