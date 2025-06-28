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
 * 订单类型
 */
@Getter
public enum OrderType {

    /**
     * Good till Cancel - equivalent to regular limit order
     * 挂单 直到撤单
     */
    GTC(0),


    /**
     * Immediate or Cancel - equivalent to strict-risk market order
     * with price cap 在价格上限下
     *
     * 立即成交否则撤单
     */
    IOC(1),

    /**
     * with total amount cap 包含价格上限
     */
    IOC_BUDGET(2),

    /**
     * Fill or Kill - execute immediately completely or not at all
     * with price cap 在价格上限下
     *
     * 立即全部成交否则取消
     */
    FOK(3), //
    /**
     * 包含价格上限
     */
    FOK_BUDGET(4); // total amount cap

    private final byte code;

    OrderType(final int code) {
        this.code = (byte) code;
    }

    public static OrderType of(final byte code) {
        switch (code) {
            case 0:
                return GTC;
            case 1:
                return IOC;
            case 2:
                return IOC_BUDGET;
            case 3:
                return FOK;
            case 4:
                return FOK_BUDGET;
            default:
                throw new IllegalArgumentException("unknown OrderType:" + code);
        }
    }

}
