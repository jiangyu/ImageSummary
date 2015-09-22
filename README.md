#FSImage Parse For Summary
很多时候我们想要解析一个FSImage文件分析当时的hdfs，但是我们并不想获得所有的inode与block，而只是想知道summary，比如有多少个INodes,有多少个Blocks，多少个Directory,多少个Files。如果使用OfflineImageViewr会占用大量的内存，很多时候甚至是不现实的，所以开发一个简单的summary程序方便查看状态十分必要。  

----

首先看一下FSImage文件的结构，FSImage文件按照Section进行存储，  
1.MAGIN_HEADER ("HDFSIMG1")   
2.FILE_VERSION,LAYOUT_VERSION  
3.SECTION  
3.1 NS_INFO Section  
包含了timestamp,lastAllocateBlockId,txid等等。  
3.2 INODES Section  
固化所有INode节点，INodeDirectory节点，FileUnderconstruction节点,以及Snapshot数据。  
3.3 DELEGATION_TOKENS Section  
3.4 CACHE_POOLS Section  
4.FileSummary  
每一个Section固化结束后都想summary写入相应的Section名称，长度及偏移量，方便读取。

所以读取的时候我们首先读取到summary中信息，根据summary中信息跳转到文件的offset部分即可读取，对于我上面所说的需求，我们感兴趣的是NS_INFO Section和INode Section,其它Section我们不读即可。  
1.读取到NS_INFO Section我们去获得timestamp,lastBlockId和TransactionId。  
2.读取到INdoes Section，我们只需记录文件个数和目录个数即可。  
工程在我的[github](https://github.com/jiangyu/ImageSummary)  
