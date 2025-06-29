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
package exchange.core2.core.common;

import lombok.Getter;

/**
 * 订单行为类型枚举
 */
@Getter
public enum OrderAction {
    // 卖出
    ASK(0),
    // 买入
    BID(1);

    /**
     * 编码
     */
    private byte code;

    OrderAction(int code) {
        this.code = (byte) code;
    }

    public static OrderAction of(byte code) {
        switch (code) {
            case 0:
                return ASK;
            case 1:
                return BID;
            default:
                throw new IllegalArgumentException("unknown OrderAction:" + code);
        }
    }


    public OrderAction opposite() {
        return this == ASK ? BID : ASK;
    }

}
