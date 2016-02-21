从[MIT 6.824 lab3](http://nil.csail.mit.edu/6.824/2015/labs/lab-3.html) 中抠出来的模拟Paxos算法以及应用的例子.

src下的paxos目录是Paxos算法及其测试用例,kvpaxos目录是基于这个Paxos库实现的一个KV存储系统.

可以分别进入对应的目录中执行
 
```
go test
```

来跑测试用例,但是在执行kvpaxos目录的用例之前需要首先设置Go的执行环境,将Paxos目录加入环境中,具体可类似env.sh脚本.

这个实验的完整描述参见最开始开始的MIT课程实验链接.