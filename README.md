# Cloud-Computing-and-Big-Data
0. 新建tools文件夹，在运行代码前需要下载JDk8安装包和Hadoop2.8.5安装包到tools文件夹下。
1. 首先，运行下面这条指令构建能够运行Hadoop的配套环境的镜像：
```Bash
docker build -t="centos-hadoop" .
```
2. 然后，运行下面这两条指令构建容器并启动Hadoop集群：
```Bash
bash creat_network.sh
bash start_container.sh
```
3. 接着将程序和数据拷贝进hadoop集群的1号节点，这里我已经上传了编译打包好的Java程序，当然也可以自己从源代码再编译一次：
```Bash
docker cp ./movie.txt hadoop-node1:/
docker cp ./itemcf.jar hadoop-node1:/
```
4. 进入hadoop集群的1号节点，并在HDFS中新建文件夹和上传输入数据：
```Bash
docker exec -it hadoop-node1 /bin/bash
hdfs dfs -mkdir /input
hdfs dfs -mkdir /output
hdfs dfs -put ./movie.txt /input
```
5. 运行java程序即可获得结果，最终结果保存在HDFS中的/output/output4/下：
```Bash
hadoop jar itemcf.jar /input/movie.txt /output/output4
```
