sourcecode 文件夹下为源代码
output 文件夹下为各个结果截图
运行模式为伪分布

# 代码解释

## 1. wordcount.java

总体上和书上接近，忽略大小写和标点只需在map函数中增加如下部分

```Java
String line = value.toString().toLowerCase();//全部变为小写
line = line.replaceAll("[\\pP\u3000]", "");//忽略标点(文本中存在\u3000这样一个全角非标点字符，也替换掉）
```

运行命令
```
wordcount <in> <out>
```

## 2. MatrixMultiply.java

由书上代码简单调整得到
```
运行命令：MatrixMultiply <inputPathM> <inputPathN> <outputPath>
```
## 3. 关系代数

对关系A（id,name,age,weight）的定义在RelationA.java中
关系B未单独定义

### （1）Selection.java

map根据条件选择满足要求的数据记录，判定是否满足的方法为iscondition(col,symbol,value),写在RelationA.java中,实现步骤为根据col找到属性列，在由symbol(符号)和value判断是否该行数据是否符合。
无需额外的reduce
```
运行命令：Selection <columnname> <symbol> <value> <inputpath> <outputpath>
```

### （2）Projection.java

通过map来获取每行数据属性col的值，输出为<col值,NullWritable>
通过reduce来合并所有键值对，这里类似于WordCount的reducer，输出<col值,NullWritable>
结果即为属性col的投影
```
运行命令：projection <columnname> <inputpath> <outputpath>
```
### （3）Unionion.java

如果两条记录line完全相同，即为相同记录，此处只需要读取为String即可比较
map输出<line,NullWritable>,再由reduce输出所有key即可
```
运行命令：Unionion <inputrelation1> <inputrelation2> <outputpath>
```
### （4）Intersection.java

交运算处理方法类似与并运算
map<line,1>,reduce合并键值对，输出所有<key,2>的key值即可
```
运行命令：intersection <inputrelation1> <inputrelation2> <outputpath>
```
### （5）Difference.java

差运算的处理方式为：
例如计算R-T
map得到<line,relationName>,即每条数据记录来自于哪个关系，relationName为R或T
reduce输入为<line,{relationName1,...}>，如果值序列的size等于1，且其中唯一一个值为R，则write这条记录
```
运行命令：Difference <inputrelation1> <inputrelation2> <outputpath>
```
### （6）NaturalJoin.java

方法和差运算有相似之处：
map得到<key,last>,其中key为选择的属性列的值（例如此处为id），last为该行数据剩下的属性和关系名relationname的组合。
reduce对于传入的每个键值对.先对value按照relationname划分为两类（例如大小分别为m,n),再得到一个m*n个result（笛卡尔积）。对每个result，write（key+result）
```
运行命令：naturaljoin <inputrelation1> <inputrelation2> <outputpath>
```
