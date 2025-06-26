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
package exchange.core2.core.orderbook;

import exchange.core2.core.common.IOrder;
import exchange.core2.core.common.MatcherEventType;
import exchange.core2.core.common.MatcherTradeEvent;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.utils.SerializationUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.NativeBytes;
import net.openhft.chronicle.wire.Wire;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static exchange.core2.core.ExchangeCore.EVENTS_POOLING;

@Slf4j
@RequiredArgsConstructor
public final class OrderBookEventsHelper {

    public static final OrderBookEventsHelper NON_POOLED_EVENTS_HELPER = new OrderBookEventsHelper(MatcherTradeEvent::new);

    // 匹配器交易事件供应函数接口
    private final Supplier<MatcherTradeEvent> eventChainsSupplier;
    // 事件链头
    private MatcherTradeEvent eventsChainHead;

    /**
     * 发送交易事件
     *
     * @param matchingOrder 匹配订单
     * @param makerCompleted 交易完成标记
     * @param takerCompleted 取样完成标记
     * @param size 取样数量
     * @param bidderHoldPrice 成交价
     *
     * @return 匹配交易事件实例
     */
    public MatcherTradeEvent sendTradeEvent(final IOrder matchingOrder,
                                            final boolean makerCompleted,
                                            final boolean takerCompleted,
                                            final long size,
                                            final long bidderHoldPrice) {
        //final long takerOrderTimestamp

//        log.debug("** sendTradeEvent: active id:{} matched id:{}", activeOrder.orderId, matchingOrder.orderId);
//        log.debug("** sendTradeEvent: price:{} v:{}", price, v);

        final MatcherTradeEvent event = newMatcherEvent();

        event.eventType = MatcherEventType.TRADE;
        event.section = 0;

        event.activeOrderCompleted = takerCompleted;

        event.matchedOrderId = matchingOrder.getOrderId();
        event.matchedOrderUid = matchingOrder.getUid();
        event.matchedOrderCompleted = makerCompleted;

        event.price = matchingOrder.getPrice();
        event.size = size;

        // set order reserved price for correct released EBids
        event.bidderHoldPrice = bidderHoldPrice;

        return event;

    }

    public MatcherTradeEvent sendReduceEvent(final IOrder order, final long reduceSize, final boolean completed) {
//        log.debug("Cancel ");
        final MatcherTradeEvent event = newMatcherEvent();
        event.eventType = MatcherEventType.REDUCE;
        event.section = 0;
        event.activeOrderCompleted = completed;
//        event.activeOrderSeq = order.seq;
        event.matchedOrderId = 0;
        event.matchedOrderCompleted = false;
        event.price = order.getPrice();
//        event.size = order.getSize() - order.getFilled();
        event.size = reduceSize;

        event.bidderHoldPrice = order.getReserveBidPrice(); // set order reserved price for correct released EBids

        return event;
    }


    public void attachRejectEvent(final OrderCommand cmd, final long rejectedSize) {

//        log.debug("Rejected {}", cmd.orderId);
//        log.debug("\n{}", getL2MarketDataSnapshot(10).dumpOrderBook());

        final MatcherTradeEvent event = newMatcherEvent();

        event.eventType = MatcherEventType.REJECT;

        event.section = 0;

        event.activeOrderCompleted = true;
//        event.activeOrderSeq = cmd.seq;

        event.matchedOrderId = 0;
        event.matchedOrderCompleted = false;

        event.price = cmd.price;
        event.size = rejectedSize;

        event.bidderHoldPrice = cmd.reserveBidPrice; // set command reserved price for correct released EBids

        // insert event
        event.nextEvent = cmd.matcherEvent;
        cmd.matcherEvent = event;
    }

    public MatcherTradeEvent createBinaryEventsChain(final long timestamp,
                                                     final int section,
                                                     final NativeBytes<Void> bytes) {

        long[] dataArray = SerializationUtils.bytesToLongArray(bytes, 5);

        MatcherTradeEvent firstEvent = null;
        MatcherTradeEvent lastEvent = null;
        for (int i = 0; i < dataArray.length; i += 5) {

            final MatcherTradeEvent event = newMatcherEvent();

            event.eventType = MatcherEventType.BINARY_EVENT;

            event.section = section;
            event.matchedOrderId = dataArray[i];
            event.matchedOrderUid = dataArray[i + 1];
            event.price = dataArray[i + 2];
            event.size = dataArray[i + 3];
            event.bidderHoldPrice = dataArray[i + 4];

            event.nextEvent = null;

//            log.debug("BIN EVENT: {}", event);

            // attach in direct order
            if (firstEvent == null) {
                firstEvent = event;
            } else {
                lastEvent.nextEvent = event;
            }
            lastEvent = event;
        }

        return firstEvent;
    }


    public static NavigableMap<Integer, Wire> deserializeEvents(final OrderCommand cmd) {

        final Map<Integer, List<MatcherTradeEvent>> sections = new HashMap<>();
        cmd.processMatcherEvents(evt -> sections.computeIfAbsent(evt.section, k -> new ArrayList<>()).add(evt));

        NavigableMap<Integer, Wire> result = new TreeMap<>();

        sections.forEach((section, events) -> {
            final long[] dataArray = events.stream()
                    .flatMap(evt -> Stream.of(
                            evt.matchedOrderId,
                            evt.matchedOrderUid,
                            evt.price,
                            evt.size,
                            evt.bidderHoldPrice))
                    .mapToLong(s -> s)
                    .toArray();

            final Wire wire = SerializationUtils.longsToWire(dataArray);

            result.put(section, wire);
        });


        return result;
    }

    /**
     * 新匹配事件
     *
     * @return 匹配事件实例
     */
    private MatcherTradeEvent newMatcherEvent() {

        // 是否启用匹配器交易事件池化功能
        if (EVENTS_POOLING) {
            // 链头为空
            if (eventsChainHead == null) {
                // 从池中获取链头
                eventsChainHead = eventChainsSupplier.get();
//            log.debug("UPDATED HEAD size={}", eventsChainHead == null ? 0 : eventsChainHead.getChainSize());
            }
            // 返回当前事件链头
            final MatcherTradeEvent res = eventsChainHead;
            // 链头游标指向链头的下一个元素,释放当前链头
            eventsChainHead = eventsChainHead.nextEvent;
            return res;
        } else {
            // 未启用池化功能直接创建新事件
            return new MatcherTradeEvent();
        }
    }

}
