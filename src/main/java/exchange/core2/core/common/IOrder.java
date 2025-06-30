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

/**
 * 订单接口
 */
public interface IOrder extends StateHash {

    /**
     * 获取订单价格
     *
     * @return
     */
    long getPrice();

    /**
     * 获取订单数量
     *
     * @return
     */
    long getSize();

    /**
     * 获取已成交量
     *
     * @return
     */
    long getFilled();

    /**
     * 获取用户ID
     *
     * @return
     */
    long getUid();

    /**
     * 获取动作
     *
     * @return
     */
    OrderAction getAction();

    /**
     * 获取订单编号
     *
     * @return
     */
    long getOrderId();

    /**
     * 获取时间戳
     *
     * @return
     */
    long getTimestamp();

    /**
     * 获取预留买入价格
     *
     * @return
     */
    long getReserveBidPrice();

}
