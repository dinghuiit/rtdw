package com.zz.bi.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class MySQLParser implements CanalParser {

    @Override
    public String processDelete(CanalEntry.RowData row, CanalEntry.Header header) {
        List<CanalEntry.Column> keys = row.getBeforeColumnsList().stream().filter(c -> c.getIsKey()).collect(Collectors.toList());
        String where = null;
        if (keys.size() == 0) {
            //无主键
            where = row.getBeforeColumnsList().stream()
                    .map(CanalParser::dealFilterNull)
                    .reduce((a, b) -> a.concat("AND").concat(b))
                    .get();
        } else {
            //有主键
            where = keys.stream()
                    .map(CanalParser::dealFilterNull)
                    .reduce((a, b) -> a.concat("AND").concat(b))
                    .get();
        }
        String sql = MessageFormat.format(" DELETE FROM {0}.{1} WHERE {2}", header.getSchemaName(), header.getTableName(), where);
        logger.debug(sql);
        return sql;
    }

    @Override
    public String processInsert(CanalEntry.RowData row, CanalEntry.Header header) {
        String insert = " INSERT INTO ".concat(header.getTableName()).concat("(");
        String values = " VALUES(";
        insert += row.getAfterColumnsList().stream()
                .map(c -> c.getName())
                .reduce((a, b) -> a.concat(", ").concat(b))
                .get().concat(")");
        values += row.getAfterColumnsList().stream()
                .map(CanalParser::dealNull)
                .reduce((a, b) -> a.concat(", ").concat(b))
                .get().concat(")");
        String sql = insert.concat(values);
        logger.debug(sql);
        return sql;
    }

}
