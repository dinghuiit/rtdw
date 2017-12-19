package com.zz.bi.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;
import java.util.stream.Collectors;

public interface CanalParser{
    Logger logger = LoggerFactory.getLogger(MySQLParser.class);

    /**
     * 处理DELETE事件
     *
     * @param row
     * @param header
     * @return
     */
    String processDelete(CanalEntry.RowData row, CanalEntry.Header header);

    /**
     * 处理INSERT事件
     *
     * @param row
     * @param header
     * @return
     */
    String processInsert(CanalEntry.RowData row, CanalEntry.Header header);

    /**
     * 处理UPDATE事件
     *
     * @param row
     * @param header
     */
    default String processUpdate(CanalEntry.RowData row, CanalEntry.Header header) {
        String sql = processDelete(row, header).concat(";").concat(processInsert(row, header));
        return  sql;
    }

    /**
     * 处理DDL
     *
     * @param rowChange
     * @param header
     * @return
     */
    default String processDDL(CanalEntry.RowChange rowChange, CanalEntry.Header header) {
        logger.debug(rowChange.getSql());
        return rowChange.getSql();
    }

    /**
     * 处理where子句中的null值
     *
     * @param col
     * @return
     */
    static String dealFilterNull(CanalEntry.Column col) {
        return col.getIsNull() ? col.getName().concat(" IS NULL") : col.getName().concat(" = ").concat(dealNull(col));
    }

    /**
     * 处理values 子句中的null
     *
     * @param col
     * @return
     */
    static String dealNull(CanalEntry.Column col) {
        String value = null;
        String jdbcType = col.getMysqlType().replaceAll("\\(.+$", "");
        switch (jdbcType) {
            case "date":
                value = col.getIsNull() ? "null" : "'".concat(col.getValue()).concat("'");
                break;
            case "int":
                value = col.getIsNull() ? "null" : col.getValue();
                break;
            case "tinyint":
                value = col.getIsNull() ? "null" : col.getValue();
                break;
            case "decimal":
                value = col.getIsNull() ? "null" : col.getValue();
                break;
            default:
                value = col.getIsNull() ? "null" : "'".concat(col.getValue()).concat("'");
                break;
        }
        return value;
    }


    /**
     * 处理一个事件日志
     *
     * @param rowChange
     * @param header
     */
    default void processRowChange(CanalEntry.RowChange rowChange, CanalEntry.Header header) {
        if (rowChange.getIsDdl()) {
            processDDL(rowChange, header);
            return;
        }
        //非DDL操作，要逐行解析数据，还原sql操作
        rowChange.getRowDatasList().stream().map(row -> {
            String sql = null;
            switch (header.getEventType()) {
                case UPDATE:
                    sql = processUpdate(row, header);
                    break;
                case DELETE:
                    sql = processDelete(row, header);
                    break;
                case INSERT:
                    sql = processInsert(row, header);
                    break;
                default:
                    System.out.println("unknown event type, skipped!");
                    break;
            }
            return sql;
        }).collect(Collectors.toList());
    }
}
