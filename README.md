# MVCC
本项目主要是为了解决内存中的并发读写问题，实现了 Snapshot 级别的事务隔离，实现了读写并发，以及不同键值对之间的并发写入。表的索引部分参考了 leveldb 中的跳跃表，实现了跳跃表结点的原子插入和惰性删除，基于乐观并发机制实现了索引部分的无锁并发读写。表的值部分按照链表的形式存储，借助全局事务序列号实现读写并发，写写悲观并发控制。

具体项目背景：在设计 rpc 服务器时，有时候会需要在进程内存储一些描述符等数据，这些数据需要由 rpc 服务器的后台线程和服务线程共同访问，需要并发读写。

# 简介
## 跳跃表

- 节点之间使用 CAS 实现原子修改，节点插入时从最底层插入，基于乐观并发机制实现插入操作的并行。
- 跳跃表节点采用原子值，实现修改操作和读取操作的并行。
- 使用惰性删除机制，将删除操作转换为修改操作。
- 利用 Compact 接口，基于 Copy On Write 技术实现已删除节点的定时释放。

## 版本链表

- 基于原子指针实现无锁链表，采用悲观并发控制实现插入的竞争，一个版本链表同时只允许一个操作。
- 限制单个操作的提交时间，防止阻塞其他操作（由于操作在内存中完成，因此不需要等待很久）。
- 操作提交阶段，检查当前活跃事务，并删除过时的版本链表部分。

## 事务控制

- 事务控制采用统一事务协调器来完成，每一个操作会分配一个版本号，从而实现事务的 Snapshot 隔离机制。
- 所有操作由事务管理器派生，提供批量写入、游标读取、简单回滚事务等功能。

## 内存表

- 提供迭代器，迭代器会使用跳跃表最底层进行遍历
- 使用后台线程对跳跃表删除节点进行压缩，压缩时不会阻塞读写

# 用户接口
## CRUD

```c++
// 初始化一个表
ValueTable table;
// 插入一个键值对
table.emplace("key", "value");
// 查找键值对是否存在
table.exist("key");
// 读取键值对
table.read("key");
// 修改键值对
table.update("key", "new_value");
//删除键值对
table.erase("key");
```

## 遍历

```C++
// 从开始进行遍历
for(auto it = table.begin();it != table.end();++it){
    auto value = *it;	// 读取值
    ...
}

// 从指定的某个值进行遍历
for(auto it = table.find("start");it != table.end();++it){
    auto value = *it; // 读取值
    ...
}

// 遍历指定的范围
for(auto it = table.find("start"),end = table.find("end");it != end;++it){
    auto value = *it; // 读取值
    ...
}
```

## 批处理

```C++
// 批量插入键值对，无法实现回滚
std::vector<std::pair<std::string, std::string>> kvs = {{"k1","v1"},{"k2","v2"}};
bool finished = table.bulkWrite(kvs);

// 使用事务插入键值对，如果失败会回滚所有操作
std::vector<std::pair<std::string, std::string>> kvs = {{"k1","v1"},{"k2","v2"}};
bool committed = table.transaction(kvs);
```

## 合并表

```c++
ValueTable table1,table2;
table1.getViewFrom(table2);
```



# 性能测试




写入 10W mvcc::Value类型空数据，std::map 680 ms ,ValueTable 755 ms。

单线程并发写入 10W 数据, std::map 花费时间500ms，跳跃表花费时间1000ms。
模拟双线程进行并发写入，std::map 结合 std::mutex 实现并发写入，在本机环境所测试时间约为700ms，跳跃表测试时间约为385ms。

# 性能瓶颈

- SkipList 进行节点插入的时候，需要等待分配内存，可以考虑加入内存预分配的机制，利用后台线程批量分配实例，需要加入的时候直接取用。
- 统一事务协调器计算当前活跃版本时，目前是借助红黑树来完成的，这里需要加锁，如果短时间内具有大量的事务会造成性能大幅度下降；目前暂未想到优化方法。

