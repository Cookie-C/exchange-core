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

import exchange.core2.core.common.*;
import lombok.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * 订单指令
 */
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public final class OrderCommand implements IOrder {

    /**
     * 订单指令类型
     */
    public OrderCommandType command;

    /**
     * 订单Id
     */
    @Getter
    public long orderId;

    /**
     * 符号
     */
    public int symbol;


    /**
     * 符号
     */
    @Getter
    public long price;

    /**
     * 数量
     */
    @Getter
    public long size;

    /**
     * 保留最低价格
     */
    @Getter
    // new orders INPUT - reserved price for fast moves of GTC bid orders in exchange mode
    public long reserveBidPrice;

    /**
     * 订单动作
     * 仅限于下单
     */
    // required for PLACE_ORDER only;
    // for CANCEL/MOVE contains original order action (filled by orderbook)
    @Getter
    public OrderAction action;

    /**
     * 订单类型
     */
    public OrderType orderType;

    @Getter
    public long uid;

    @Getter
    public long timestamp;

    public int userCookie;

    // 分组处理器填充 filled by grouping processor:
    public long eventsGroup;
    public int serviceFlags;

    //  result code of command execution - can also be used for saving intermediate state
    //  命令执行的结果代码也可以用于保存中间状态
    public CommandResultCode resultCode;

    // trade events chain 交易事件链
    public MatcherTradeEvent matcherEvent;

    // optional market data
    // 可选市场数据
    public L2MarketData marketData;

    // sequence of last available for this command
    //public long matcherEventSequence;
    // ---- potential false sharing section ------

    public static OrderCommand newOrder(OrderType orderType, long orderId, long uid, long price, long reserveBidPrice, long size, OrderAction action) {
        OrderCommand cmd = new OrderCommand();
        cmd.command = OrderCommandType.PLACE_ORDER;
        cmd.orderId = orderId;
        cmd.uid = uid;
        cmd.price = price;
        cmd.reserveBidPrice = reserveBidPrice;
        cmd.size = size;
        cmd.action = action;
        cmd.orderType = orderType;
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        return cmd;
    }

    public static OrderCommand cancel(long orderId, long uid) {
        OrderCommand cmd = new OrderCommand();
        cmd.command = OrderCommandType.CANCEL_ORDER;
        cmd.orderId = orderId;
        cmd.uid = uid;
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        return cmd;
    }

    public static OrderCommand reduce(long orderId, long uid, long reduceSize) {
        OrderCommand cmd = new OrderCommand();
        cmd.command = OrderCommandType.REDUCE_ORDER;
        cmd.orderId = orderId;
        cmd.uid = uid;
        cmd.size = reduceSize;
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        return cmd;
    }

    public static OrderCommand update(long orderId, long uid, long price) {
        OrderCommand cmd = new OrderCommand();
        cmd.command = OrderCommandType.MOVE_ORDER;
        cmd.orderId = orderId;
        cmd.uid = uid;
        cmd.price = price;
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        return cmd;
    }

    /**
     * Handles full MatcherTradeEvent chain, without removing/revoking them
     *
     * @param handler - MatcherTradeEvent handler
     */
    public void processMatcherEvents(Consumer<MatcherTradeEvent> handler) {
        MatcherTradeEvent mte = this.matcherEvent;
        while (mte != null) {
            handler.accept(mte);
            mte = mte.nextEvent;
        }
    }

    /**
     * Produces garbage
     * For testing only !!!
     *
     * @return list of events
     */
    public List<MatcherTradeEvent> extractEvents() {
        List<MatcherTradeEvent> list = new ArrayList<>();
        processMatcherEvents(list::add);
        return list;
    }

    // Traverse and remove:
//    private void cleanMatcherEvents() {
//        MatcherTradeEvent ev = this.matcherEvent;
//        this.matcherEvent = null;
//        while (ev != null) {
//            MatcherTradeEvent tmp = ev;
//            ev = ev.nextEvent;
//            tmp.nextEvent = null;
//        }
//    }
//


    /**
     * Write only command data, not status or events
     *
     * @param cmd2 command to overwrite to
     */
    public void writeTo(OrderCommand cmd2) {
        cmd2.command = this.command;
        cmd2.orderId = this.orderId;
        cmd2.symbol = this.symbol;
        cmd2.uid = this.uid;
        cmd2.timestamp = this.timestamp;

        cmd2.reserveBidPrice = this.reserveBidPrice;
        cmd2.price = this.price;
        cmd2.size = this.size;
        cmd2.action = this.action;
        cmd2.orderType = this.orderType;
    }

    // slow - testing only
    public OrderCommand copy() {

        OrderCommand newCmd = new OrderCommand();
        writeTo(newCmd);
        newCmd.resultCode = this.resultCode;

        List<MatcherTradeEvent> events = extractEvents();

//        System.out.println(">>> events: " + events);
        for (MatcherTradeEvent event : events) {
            MatcherTradeEvent copy = event.copy();
            copy.nextEvent = newCmd.matcherEvent;
            newCmd.matcherEvent = copy;
//            System.out.println(">>> newCmd.matcherEvent: " + newCmd.matcherEvent);
        }

        if (marketData != null) {
            newCmd.marketData = marketData.copy();
        }

//        System.out.println(">>> newCmd: " + newCmd);
        return newCmd;
    }

    @Override
    public long getFilled() {
        return 0;
    }

    @Override
    public int stateHash() {
        throw new UnsupportedOperationException("Command does not represents state");
    }
}
