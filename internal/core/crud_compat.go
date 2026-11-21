package core

// powersync_crud 是官方 core 暴露给触发器/应用写待上传队列的对象（core 里是虚拟表，
// 列：op, id, type, data, old_values, metadata, options[hidden]）。
//
// modernc 不提供虚拟表 API，这里用「真实暂存表 + AFTER INSERT 桥接触发器」实现等价语义：
// 写入 powersync_crud 的行会在同一事务里被搬进 ps_crud（带当前 tx_id）并从暂存表删除。
//
// 与 core 的差异：不解释 options 位（core 用其中的 ignore_empty_update 等标志），
// 即空 PATCH 也会照常入队（等价于官方默认 ignore_empty_update=false）。
var powersyncCrudDDL = []string{
	`CREATE TABLE IF NOT EXISTS powersync_crud(
	   op TEXT, id TEXT, type TEXT, data TEXT, old_values TEXT, metadata TEXT, options INT);`,
	`CREATE TRIGGER IF NOT EXISTS powersync_crud_bridge AFTER INSERT ON powersync_crud BEGIN
	   -- data/old/metadata 为 NULL 时把对应键去掉（json_remove 对不存在的路径是 no-op）；
	   -- 用 json_insert 而不是 json_patch：json_patch 会把 data 里的 null 当成"删除该键"。
	   INSERT INTO ps_crud(data, tx_id) VALUES (
	     json_remove(
	       json_insert(
	         json_insert(
	           json_insert(json_object('op', NEW.op, 'type', NEW.type, 'id', NEW.id),
	             '$.data', json(NEW.data)),
	           '$.old', json(NEW.old_values)),
	         '$.metadata', NEW.metadata),
	       IIF(NEW.data IS NULL, '$.data', '$.__absent__'),
	       IIF(NEW.old_values IS NULL, '$.old', '$.__absent__'),
	       IIF(NEW.metadata IS NULL, '$.metadata', '$.__absent__')),
	     (SELECT current_tx FROM ps_tx));
	   DELETE FROM powersync_crud WHERE rowid = NEW.rowid;
	 END;`,
}
