# M1—M3 数据库迁移

使用本项目migrator账号运行：

```bash
scripts/behaviorctl --baseline UBA-DOC-1.0.0 --credentials var/secrets/migrator.cnf migrate
```

001建立批次、任务、租约、质量控制；002建立M2分析结果表；003建立M3实验来源表。迁移前用mariadb-dump备份至对应工作目录，schema_migration记录文件SHA256。已应用文件不得修改；DDL中断的部分迁移须核对备份和实际表结构，不静默假装已回滚。

M2结果权威先位于HDFS；结果表按冻结release_id设计，但M5尚未建立release与装载流程，所以本阶段不伪造release向表中装载。M5将建立发布表并补充关联约束。M3实验仅PENDING，没有模型或测试指标。

测试使用独立behavior_test_m123数据库、/behavior-test/m123 HDFS根及var/tests/m123；账号由scripts/setup-test-database.py在本项目私有实例创建。不得使用旧项目数据库或正式根运行合成数据。
