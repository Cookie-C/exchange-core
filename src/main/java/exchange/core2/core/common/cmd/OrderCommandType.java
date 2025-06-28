/*
 * Copyright 2019 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package exchange.core2.core.common.cmd;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.HashMap;

/**
 * 订单指令类型枚举
 */
@Getter
@AllArgsConstructor
public enum OrderCommandType {

    /**
     * 下单
     */
    PLACE_ORDER((byte) 1, true),
    /**
     * 取消订单
     */
    CANCEL_ORDER((byte) 2, true),

    /**
     * 改价
     */
    MOVE_ORDER((byte) 3, true),
    /**
     * 减量
     */
    REDUCE_ORDER((byte) 4, true),
    /**
     * 订单薄请求
     */
    ORDER_BOOK_REQUEST((byte) 6, false),





    /**
     * 新增用户
     */
    ADD_USER((byte) 10, true),
    /**
     * 平衡调整
     */
    BALANCE_ADJUSTMENT((byte) 11, true),
    /**
     * 暂停用户
     */
    SUSPEND_USER((byte) 12, true),
    /**
     * 恢复用户
     */
    RESUME_USER((byte) 13, true),



    /**
     * 二进制数据查询
     */
    BINARY_DATA_QUERY((byte) 90, false),
    /**
     * 二进制数据命令
     */
    BINARY_DATA_COMMAND((byte) 91, true),


    /**
     * 持久状态匹配
     */
    PERSIST_STATE_MATCHING((byte) 110, true),
    /**
     * 持久状态风险
     */
    PERSIST_STATE_RISK((byte) 111, true),
    /**
     * 分组控制
     */
    GROUPING_CONTROL((byte) 118, false),
    /**
     * 空操作
     */
    NOP((byte) 120, false),
    /**
     * 重置
     */
    RESET((byte) 124, true),
    /**
     * 关闭信号
     */
    SHUTDOWN_SIGNAL((byte) 127, false),
    /**
     * 预留压缩状态
     */
    RESERVED_COMPRESSED((byte) -1, false);

    private final byte code;
    private final boolean mutate;

    public static OrderCommandType fromCode(byte code) {
        // TODO try if-else
        final OrderCommandType result = codes.get(code);
        if (result == null) {
            throw new IllegalArgumentException("Unknown order command type code:" + code);
        }
        return result;
    }

    private static HashMap<Byte, OrderCommandType> codes = new HashMap<>();

    static {
        for (OrderCommandType x : values()) {
            codes.put(x.code, x);
        }
    }


}
